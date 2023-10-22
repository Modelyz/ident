{-# LANGUAGE OverloadedStrings #-}

import Connection (Connection (..))
import Control.Concurrent (Chan, MVar, dupChan, forkIO, newChan, newMVar, putMVar, readChan, readMVar, takeMVar, threadDelay, writeChan)
import Control.Exception (AsyncException (..), Handler (..), SomeException (..), catches)
import Control.Monad qualified as Monad (forever, when)
import Data.Aeson qualified as JSON (eitherDecode, encode)
import Data.Map.Strict as Map (Map, delete, empty, insert)
import Data.Map.Strict qualified as Map
import Data.Maybe qualified as Maybe
import Data.Set as Set (Set, empty, insert)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.UUID.V4 qualified as UUID (nextRandom)
import Ident.Fragment (Fragment (..))
import Message (Message (Message), appendMessage, creator, getFragments, messageId, metadata, payload, readMessages, setCreator, setFlow, setFragments)
import MessageFlow (MessageFlow (..))
import MessageId (MessageId)
import Metadata (Metadata (..))
import Network.WebSockets (ConnectionException (..))
import Network.WebSockets qualified as WS (ClientApp, DataMessage (..), fromLazyByteString, receiveDataMessage, runClient, sendTextData)
import Options.Applicative qualified as Options
import Payload (Payload (..))
import Service (Service (..))
import System.Exit (exitSuccess)

-- dir, port, file
data Options = Options !FilePath !Host !Port

type Host = String
type Port = Int

data State = State
    { lastNumbers :: Map.Map T.Text Int -- last identification number for each fragment name
    , pending :: Map MessageId Message
    , uuids :: Set MessageId
    , session :: Bool
    }
    deriving (Show)
type StateMV = MVar State

myself :: Service
myself = Ident

emptyState :: State
emptyState =
    State
        { lastNumbers = Map.empty
        , pending = Map.empty
        , Main.uuids = Set.empty
        , session = False
        }

options :: Options.Parser Options
options =
    Options
        <$> Options.strOption
            ( Options.short 'f'
                <> Options.long "file"
                <> Options.value "data/messagestore.txt"
                <> Options.help "Filename of the file containing messages"
            )
        <*> Options.strOption
            ( Options.short 'h'
                <> Options.long "store_host"
                <> Options.value "localhost"
                <> Options.help "Hostname of the Store service. [default: localhost]"
            )
        <*> Options.option
            Options.auto
            ( Options.long "store_port"
                <> Options.metavar "STORE_PORT"
                <> Options.value 8081
                <> Options.help "Port of the Store service.  [default: 8081]"
            )

clientApp :: FilePath -> Chan Message -> StateMV -> WS.ClientApp ()
clientApp msgPath storeChan stateMV conn = do
    putStrLn "Connected!"
    -- Just reconnected, first send an InitiatedConnection to the store
    newUuid <- UUID.nextRandom
    currentTime <- getPOSIXTime
    state <- readMVar stateMV
    -- send an initiatedConnection
    let initiatedConnection =
            Message
                (Metadata{uuid = newUuid, Metadata.when = currentTime, Metadata.from = [myself], Metadata.flow = Requested})
                (InitiatedConnection (Connection{lastMessageTime = 0, Connection.uuids = Main.uuids state}))
    _ <- WS.sendTextData conn $ JSON.encode initiatedConnection
    -- fork a thread to send back data from the channel to the central store
    -- CLIENT WORKER THREAD
    _ <- forkIO $ do
        Monad.forever $ do
            msg <- readChan storeChan -- here we get all messages
            putStrLn $ "CLIENT WORKER THREAD received this msg from the chan:\n" ++ show msg
            case flow (metadata msg) of
                Requested -> case creator msg of
                    Front -> do
                        -- process
                        processedMsgs <- processMessage stateMV msg
                        st <- takeMVar stateMV
                        putMVar stateMV $! foldl update st processedMsgs
                        -- send to the Store
                        mapM_ (WS.sendTextData conn . JSON.encode) processedMsgs
                        putStrLn $ "Sent back this msg to the store: " ++ show processedMsgs
                        mapM_ (appendMessage msgPath) processedMsgs
                    _ -> return ()
                _ -> return ()

    -- CLIENT MAIN THREAD
    -- loop on the handling of messages incoming through websocket
    Monad.forever $ do
        message <- WS.receiveDataMessage conn
        putStrLn $ "CLIENT MAIN THREAD received the msg from Store:\n" ++ show message
        case JSON.eitherDecode
            ( case message of
                WS.Text bs _ -> WS.fromLazyByteString bs
                WS.Binary bs -> WS.fromLazyByteString bs
            ) of
            Right msg -> do
                st' <- readMVar stateMV
                case flow (metadata msg) of
                    Requested -> case creator msg of
                        Front -> Monad.when (messageId msg `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            -- Add it or remove to the pending list (if relevant) and keep the uuid
                            st'' <- takeMVar stateMV
                            putMVar stateMV $! update st'' msg
                            putStrLn "updated state"
                            -- send msg to the worker thread and to other connected clients
                            putStrLn "Writing to the chan"
                            writeChan storeChan msg
                        _ -> return ()
                    Processed -> case payload msg of
                        InitiatedConnection _ -> do
                            st''' <- takeMVar stateMV
                            putMVar stateMV $! st'''{session = True}
                            putStrLn "Got authorization from Store" -- still fake
                        _ -> Monad.when (messageId msg `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            -- Add it or remove to the pending list (if relevant) and keep the uuid
                            st'' <- takeMVar stateMV
                            putMVar stateMV $! update st'' msg
                            putStrLn "updated state"
                            -- send msg to the worker thread and to other connected clients
                            putStrLn "Writing to the chan"
                            writeChan storeChan msg
                    _ -> return ()
            Left err -> putStrLn $ "### ERROR ### decoding incoming message:\n" ++ err

update :: State -> Message -> State
update state msg =
    case flow (metadata msg) of
        Requested -> case payload msg of
            InitiatedConnection _ -> state
            _ ->
                state
                    { pending = Map.insert (messageId msg) msg $ pending state
                    , Main.uuids = Set.insert (messageId msg) (Main.uuids state)
                    }
        Processed ->
            state
                { pending = Map.delete (messageId msg) $ pending state
                , Main.uuids = Set.insert (messageId msg) (Main.uuids state)
                -- , lastNumbers = Set.insert name Maybe.fromMaybe start (Map.lookup name (lastNumbers st))
                }
        Error _ -> state

processMessage :: StateMV -> Message -> IO [Message]
processMessage stateMV msg = do
    case payload msg of
        AddedIdentifier _ -> do
            state <- takeMVar stateMV
            -- read the fragments
            let fragments = getFragments msg
            putStrLn $ "FRAGMENTS" ++ show fragments
            let (fragments', newState) =
                    foldl
                        ( \(frags, st) fragment -> case fragment of
                            Sequence name padding step start _ ->
                                let newseq = step + Maybe.fromMaybe start (Map.lookup name (lastNumbers st))
                                 in (Sequence name padding step start (Just newseq) : frags, (st{lastNumbers = Map.insert name newseq (lastNumbers st)}))
                            fr -> (fr : frags, st)
                        )
                        ([], state)
                        fragments
            putMVar stateMV $! update newState msg
            putStrLn $ "NEW STATE:\n" ++ show newState
            -- build a ProcessedMsg with the computed sequences.
            -- We need to loop on the fragment and update those whose with the right name
            state' <- takeMVar stateMV
            let processedMsg = setFlow Processed $ setFragments (reverse fragments') $ setCreator myself msg
            putMVar stateMV $! update state' processedMsg
            putStrLn $ "FRAGMENTS:\n" ++ show fragments'
            return [processedMsg]
        AddedIdentifierType _ -> return [setFlow Processed $ setCreator myself msg]
        RemovedIdentifierType _ -> return [setFlow Processed $ setCreator myself msg]
        ChangedIdentifierType _ _ -> return [setFlow Processed $ setCreator myself msg]
        _ -> return []

maxWait :: Int
maxWait = 10

reconnectClient :: Int -> POSIXTime -> Host -> Port -> FilePath -> Chan Message -> StateMV -> IO ()
reconnectClient waitTime previousTime host port msgPath storeChan stateMV = do
    putStrLn $ "Waiting " ++ show waitTime ++ " seconds"
    threadDelay $ waitTime * 1000000
    putStrLn $ "Connecting to Store at ws://" ++ host ++ ":" ++ show port ++ "..."

    catches
        (WS.runClient host port "/" (clientApp msgPath storeChan stateMV))
        [ Handler
            (\(_ :: ConnectionException) -> reconnectClient 1 previousTime host port msgPath storeChan stateMV)
        , Handler
            ( \(e :: AsyncException) -> case e of
                UserInterrupt -> do
                    putStrLn "Stopping..."
                    exitSuccess
                _ -> return ()
            )
        , Handler
            ( \(_ :: SomeException) ->
                do
                    disconnectTime <- getPOSIXTime
                    let newWaitTime = if fromEnum (disconnectTime - previousTime) >= (1000000000000 * (maxWait + 1)) then 1 else min maxWait $ waitTime + 1
                    reconnectClient newWaitTime disconnectTime host port msgPath storeChan stateMV
            )
        ]

serve :: Options -> IO ()
serve (Options msgPath storeHost storePort) = do
    chan <- newChan -- main channel, that will be duplicated for the store
    stateMV <- newMVar emptyState
    firstTime <- getPOSIXTime
    storeChan <- dupChan chan -- output channel to the central message store
    -- Reconstruct the state
    putStrLn "Reconstructing the State..."
    msgs <- readMessages msgPath
    state <- takeMVar stateMV
    let newState = foldl update state msgs -- TODO foldr or strict foldl ?
    putMVar stateMV newState
    putStrLn $ "STATE:\n" ++ show newState
    -- keep connection to the Store
    reconnectClient 1 firstTime storeHost storePort msgPath storeChan stateMV

main :: IO ()
main =
    serve =<< Options.execParser opts
  where
    opts =
        Options.info
            (options Options.<**> Options.helper)
            ( Options.fullDesc
                <> Options.progDesc "Ident handles the identification needs"
                <> Options.header "Modelyz Ident"
            )

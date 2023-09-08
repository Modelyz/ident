{-# LANGUAGE OverloadedStrings #-}

import Connection (Connection (..))
import Control.Concurrent (Chan, MVar, dupChan, forkIO, newChan, newMVar, putMVar, readChan, readMVar, takeMVar, threadDelay, writeChan)
import Control.Exception (AsyncException (..), Handler (..), SomeException (..), catches)
import Control.Monad qualified as Monad (forever, when)
import Data.Aeson qualified as JSON (eitherDecode, encode)
import Data.Map.Strict qualified as Map
import Data.Maybe qualified as Maybe
import Data.Set as Set (Set, delete, empty, insert)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.UUID.V4 qualified as UUID (nextRandom)
import Ident.Fragment (Fragment (..))
import Message (Message (Message), Payload (..), appendMessage, getFragments, metadata, payload, readMessages, setCreator, setFlow, setFragments)
import MessageFlow (MessageFlow (..))
import Metadata (Metadata (..), Origin (..))
import Network.WebSockets (ConnectionException (..))
import Network.WebSockets qualified as WS (ClientApp, DataMessage (..), fromLazyByteString, receiveDataMessage, runClient, sendTextData)
import Options.Applicative qualified as Options
import System.Exit (exitSuccess)

-- dir, port, file
data Options = Options !FilePath !Host !Port

type Host = String
type Port = Int

data State = State
    { lastNumbers :: Map.Map T.Text Int -- last identification number for each fragment name
    , pending :: Set Message
    , uuids :: Set Metadata
    }
    deriving (Show)
type StateMV = MVar State

emptyState :: State
emptyState =
    State
        { lastNumbers = Map.empty
        , pending = Set.empty
        , Main.uuids = Set.empty
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
                (Metadata{uuid = newUuid, Metadata.when = currentTime, Metadata.from = Ident, Metadata.flow = Requested})
                (InitiatedConnection (Connection{lastMessageTime = 0, Connection.uuids = Main.uuids state}))
    _ <- WS.sendTextData conn $ JSON.encode initiatedConnection
    -- Just reconnected, send the pending messages to the Store
    mapM_ (WS.sendTextData conn . JSON.encode) (pending state)
    _ <- forkIO $ do
        putStrLn "Waiting for messages coming from the Store"
        Monad.forever $ do
            msg <- readChan storeChan -- here we get all messages from all browsers
            Monad.when (from (metadata msg) == Front) $ do
                case flow (metadata msg) of
                    Requested -> do
                        putStrLn $ "\nProcessing this msg coming from browser: " ++ show msg
                        st <- takeMVar stateMV
                        -- process
                        processedMsg <- processMessage stateMV msg
                        putMVar stateMV $! foldl update st processedMsg
                        -- send to the Store
                        putStrLn $ "Send back this msg to the store: " ++ show processedMsg
                        mapM_ (WS.sendTextData conn . JSON.encode) processedMsg
                    _ -> return ()

    -- CLIENT MAIN THREAD
    -- loop on the handling of messages incoming through websocket
    putStrLn "Starting message handler"
    Monad.forever $ do
        message <- WS.receiveDataMessage conn
        putStrLn $ "\nReceived msg through websocket from the store: " ++ show message
        case JSON.eitherDecode
            ( case message of
                WS.Text bs _ -> WS.fromLazyByteString bs
                WS.Binary bs -> WS.fromLazyByteString bs
            ) of
            Right msg -> do
                case flow (metadata msg) of
                    Requested -> do
                        st' <- readMVar stateMV
                        Monad.when (from (metadata msg) == Front && metadata msg `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            -- send msg to other connected clients
                            putStrLn "\nWriting to the chan"
                            writeChan storeChan msg
                            -- Add it or remove to the pending list (if relevant) and keep the uuid
                            st'' <- takeMVar stateMV
                            putMVar stateMV $! update st'' msg
                            putStrLn "updated state"
                    _ -> return ()
            Left err -> putStrLn $ "\nError decoding incoming message" ++ err

update :: State -> Message -> State
update state msg =
    case flow (metadata msg) of
        Requested -> case payload msg of
            InitiatedConnection _ -> state
            _ ->
                state
                    { pending = Set.insert msg $ pending state
                    , Main.uuids = Set.insert (metadata msg) (Main.uuids state)
                    }
        Processed -> state{pending = Set.delete msg $ pending state}
        Error _ -> state

processMessage :: StateMV -> Message -> IO [Message]
processMessage stateMV msg = do
    case payload msg of
        AddedIdentifier _ -> do
            -- store the ident messages in the local store
            putStrLn $ "\nStored message: " ++ show msg
            state <- takeMVar stateMV
            -- read the fragments
            let fragments = getFragments msg
            print fragments
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
            putStrLn $ "\nnewState = " ++ show newState
            -- build a ProcessedMsg with the computed sequences.
            -- We need to loop on the fragment and update those whose with the right name
            state' <- takeMVar stateMV
            let processedMsg = setFlow Processed $ setFragments (reverse fragments') $ setCreator Ident msg
            putMVar stateMV $! update state' processedMsg
            putStrLn $ "\nfragments: " ++ show fragments'
            return [processedMsg]
        AddedIdentifierType _ -> return [setFlow Processed $ setCreator Ident msg]
        RemovedIdentifierType _ -> return [setFlow Processed $ setCreator Ident msg]
        ChangedIdentifierType _ _ -> return [setFlow Processed $ setCreator Ident msg]
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
    putStrLn $ "Computed State:" ++ show newState
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

{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent (threadDelay)
import Control.Concurrent qualified as CC
import Control.Exception (AsyncException (..), Handler (..), SomeException (..), catches)
import Control.Monad qualified as Monad (forever, unless)
import Data.Aeson qualified as JSON (decode, encode)
import Data.Map.Strict qualified as Map
import Data.Maybe qualified as Maybe
import Data.Text qualified as T
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Ident.Fragment (Fragment (..))
import Message (Message, Payload (..), appendMessage, getFragments, isProcessed, payload, setCreator, setFlow, setFragments)
import MessageFlow (MessageFlow (..))
import Network.WebSockets qualified as WS (ClientApp, Connection, DataMessage (..), fromLazyByteString, receiveDataMessage, runClient, sendTextData)
import Options.Applicative qualified as Options
import System.Exit (exitSuccess)

-- dir, port, file
data Options = Options !FilePath !Host !Port

type Host = String
type Port = Int

newtype State = State {lastNumbers :: Map.Map T.Text Int}
    deriving (Show)
type StateMV = CC.MVar State

emptyState :: State
emptyState = State{lastNumbers = Map.empty}

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

clientApp :: FilePath -> StateMV -> WS.ClientApp ()
clientApp f stateMV conn = do
    putStrLn "Connected!"
    -- TODO: Use the Flow to determine if it has been received by the store, in case the store was not alive.
    --
    -- loop on the handling of messages incoming through websocket
    putStrLn "Starting message handler"
    Monad.forever $ do
        msg <- WS.receiveDataMessage conn
        putStrLn $ "\nReceived string through websocket from store: " ++ show msg
        case JSON.decode
            ( case msg of
                WS.Text bs _ -> WS.fromLazyByteString bs
                WS.Binary bs -> WS.fromLazyByteString bs
            ) of
            Just ev -> handleMessage f conn stateMV ev
            Nothing -> putStrLn "\nError decoding incoming message"

handleMessage :: FilePath -> WS.Connection -> StateMV -> Message -> IO ()
handleMessage f conn stateMV msg = do
    Monad.unless (isProcessed msg) $ do
        case payload msg of
            AddedIdentifier _ -> do
                -- store the ident messages in the local store
                appendMessage f msg
                putStrLn $ "\nStored message: " ++ show msg
                -- read the fragments
                let fragments = getFragments msg
                print fragments
                state <- CC.takeMVar stateMV
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
                CC.putMVar stateMV $! newState
                putStrLn $ "\nnewseqMap = " ++ show newState
                -- build an msg' with the computed sequences.
                -- We need to loop on the fragment and update those whose with the right name
                let msg' = setFlow Processed $ setFragments (reverse fragments') $ setCreator "ident" msg
                putStrLn $ "\nfragments: " ++ show fragments'
                -- Store and send back an ACK to let the client know the message has been processed
                -- except for messages that already have an ACK
                appendMessage f msg'
                WS.sendTextData conn $ JSON.encode msg'
                putStrLn $ "\nSent msg' through WS: " ++ show msg'
            AddedIdentifierType _ -> do
                appendMessage f msg
                processMessage f conn msg
            RemovedIdentifierType _ -> do
                appendMessage f msg
                processMessage f conn msg
            ChangedIdentifierType _ _ -> do
                appendMessage f msg
                processMessage f conn msg
            _ -> putStrLn "Invalid message received. Not handling it."

processMessage :: FilePath -> WS.Connection -> Message -> IO ()
processMessage f conn msg = do
    -- just set as Processed, store and send back
    let msg' = setFlow Processed $ setCreator "ident" msg
    appendMessage f msg'
    WS.sendTextData conn $ JSON.encode msg'
    putStrLn $ "\nSent msg' through WS: " ++ show msg'

maxWait :: Int
maxWait = 10

connectClient :: Int -> POSIXTime -> Host -> Port -> FilePath -> StateMV -> IO ()
connectClient waitTime previousTime host port msgPath stateMV = do
    putStrLn $ "Waiting " ++ show waitTime ++ " seconds"
    threadDelay $ waitTime * 1000000
    putStrLn $ "Connecting to Store at ws://" ++ host ++ ":" ++ show port ++ "..."

    catches
        (WS.runClient host port "/" (clientApp msgPath stateMV))
        [ Handler
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
                    connectClient newWaitTime disconnectTime host port msgPath stateMV
            )
        ]

{-( \(e :: AsyncException) -> case e of
        UserInterrupt -> do
            putStrLn "Stopping..."
            exitSuccess
        _ -> putStrLn "bop"
    )
)
( do
    putStrLn "plop"
    disconnectTime <- getPOSIXTime
    let newWaitTime = if fromEnum (disconnectTime - previousTime) >= (1000000000000 * (maxWait + 1)) then 1 else min maxWait $ waitTime + 1
    connectClient newWaitTime disconnectTime host port msgPath stateMV
)-}

serve :: Options -> IO ()
serve (Options msgPath storeHost storePort) = do
    stateMV <- CC.newMVar emptyState
    firstTime <- getPOSIXTime
    connectClient 1 firstTime storeHost storePort msgPath stateMV

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

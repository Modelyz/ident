{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent (threadDelay)
import Control.Concurrent qualified as CC
import Control.Exception (SomeException (SomeException), catch)
import Control.Monad qualified as Monad (forever, when)
import Data.Aeson qualified as JSON (decode, encode)
import Data.Map.Strict qualified as Map
import Data.Maybe qualified as Maybe
import Data.Text qualified as T
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Traversable qualified as Traversable (sequence)
import Ident.Fragment (Fragment (..))
import Message (Message, appendMessage, getFragments, isProcessed, isType, setFlow, setFragments)
import MessageFlow (MessageFlow (..))
import Network.WebSockets qualified as WS
import Options.Applicative qualified as Options

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
        messages <- WS.receiveDataMessage conn
        putStrLn $ "\nReceived string through websocket from store: " ++ show messages
        case JSON.decode
            ( case messages of
                WS.Text bs _ -> WS.fromLazyByteString bs
                WS.Binary bs -> WS.fromLazyByteString bs
            ) of
            Just evs -> mapM (handleMessage f conn stateMV) evs
            Nothing -> Traversable.sequence [putStrLn "\nError decoding incoming message"]

handleMessage :: FilePath -> WS.Connection -> StateMV -> Message -> IO ()
handleMessage f conn stateMV ev = do
    Monad.when (ev `isType` "AddedIdentifier" && not (isProcessed ev)) $ do
        -- store the ident messages in the local store
        appendMessage f ev
        putStrLn $ "\nStored message: " ++ show ev
        -- read the fragments
        let fragments = getFragments ev
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
        -- build an ev' with the computed sequences.
        -- We need to loop on the fragment and update those whose with the right name
        let ev' = setFlow Processed $ setFragments (reverse fragments') ev
        putStrLn $ "\nfragments: " ++ show fragments'
        -- Store and send back an ACK to let the client know the message has been processed
        -- except for messages that already have an ACK
        appendMessage f ev'
        WS.sendTextData conn $ JSON.encode [ev']
        putStrLn $ "\nSent ev' through WS: " ++ show ev'

maxWait :: Int
maxWait = 10

connectClient :: Int -> POSIXTime -> Host -> Port -> FilePath -> StateMV -> IO ()
connectClient waitTime previousTime host port msgPath stateMV = do
    putStrLn $ "Waiting " ++ show waitTime ++ " seconds"

    threadDelay $ waitTime * 1000000
    putStrLn $ "Connecting to Store at ws://" ++ host ++ ":" ++ show port ++ "..."
    catch
        (WS.runClient host port "/" (clientApp msgPath stateMV))
        ( \(SomeException _) -> do
            disconnectTime <- getPOSIXTime
            let newWaitTime = if fromEnum (disconnectTime - previousTime) >= (1000000000000 * (maxWait + 1)) then 1 else min maxWait $ waitTime + 1
            connectClient newWaitTime disconnectTime host port msgPath stateMV
        )

serve :: Options -> IO ()
serve (Options msgPath storeHost storePort) = do
    stateMV <- CC.newMVar emptyState
    firstTime <- getPOSIXTime
    putStrLn $ "Connecting to Store at ws://" ++ storeHost ++ ":" ++ show storePort ++ "/"
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

{-# LANGUAGE OverloadedStrings #-}

import qualified Control.Concurrent as CC
import qualified Control.Monad as Monad (forever, when)
import qualified Data.Aeson as JSON (decode, encode)
import qualified Data.Map.Strict as Map
import qualified Data.Maybe as Maybe
import qualified Data.Traversable as Traversable (sequence)
import Ident.Fragment (Fragment (..), getFragments, setFragments)
import Message (Message, appendMessage, isProcessed, isType, setFlow)
import qualified Network.WebSockets as WS
import Options.Applicative

-- dir, port, file
data Options = Options !FilePath !Host !Port

type Host = String
type Port = Int

newtype State = State {lastNumbers :: Map.Map String Int}
    deriving (Show)
type StateMV = CC.MVar State

emptyState :: State
emptyState = State{lastNumbers = Map.empty}

options :: Parser Options
options =
    Options
        <$> strOption
            ( short 'f'
                <> long "file"
                <> value "messagestore.txt"
                <> help "Filename of the file containing messages"
            )
        <*> strOption
            ( short 'h'
                <> long "store_host"
                <> value "localhost"
                <> help "Hostname of the Store service. [default: localhost]"
            )
        <*> option
            auto
            ( long "store_port"
                <> metavar "STORE_PORT"
                <> value 8081
                <> help "Port of the Store service.  [default: 8081]"
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
    Monad.when (isType "AddedIdentifier" ev && not (isProcessed ev)) $ do
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
        let ev' = setFlow "Processed" $ setFragments (reverse fragments') ev
        putStrLn $ "\nfragments: " ++ show fragments'
        -- Store and send back an ACK to let the client know the message has been processed
        -- except for messages that already have an ACK
        appendMessage f ev'
        WS.sendTextData conn $ JSON.encode [ev']
        putStrLn $ "\nSent ev' through WS: " ++ show ev'

serve :: Options -> IO ()
serve (Options f h p) = do
    stateMV <- CC.newMVar emptyState
    putStrLn $ "Connecting to Store at ws://" ++ h ++ ":" ++ show p ++ "/"
    WS.runClient h p "/" (clientApp f stateMV) -- TODO auto-reconnect

main :: IO ()
main =
    serve =<< execParser opts
  where
    opts =
        info
            (options <**> helper)
            ( fullDesc
                <> progDesc "Ident handles the identification needs"
                <> header "Modelyz Ident"
            )

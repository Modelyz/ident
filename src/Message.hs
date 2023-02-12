{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Message (Fragment (..), getUuids, Uuid, Message, getInt, setProcessed, getMetaString, excludeType, isProcessed, isType, isAfter, getFragments, setFragments) where

import Data.Aeson as JSON (FromJSON, Result (..), ToJSON, Value (..), fromJSON, object, parseJSON, toJSON, withObject, (.:), (.=))
import Data.Aeson.Key (Key)
import Data.Aeson.KeyMap as KeyMap (alterF, lookup)
import Data.Aeson.Types (parseFail)
import Data.Scientific (toBoundedInteger)
import qualified Data.Text as T (Text, pack, unpack)
import Data.Time.Calendar.Month (Month (..))
import Data.Time.Calendar.WeekDate (DayOfWeek (..))
import Data.Time.Clock (diffTimeToPicoseconds)
import Data.Time.Clock.POSIX (POSIXTime)
import qualified Data.Vector as Vector
import GHC.Generics

type Message = JSON.Value

type Time = Int

type Uuid = String -- uuid as a string generated in Elm

data Fragment
    = Free String
    | Fixed String
    | Sequence String Int Int Int (Maybe Int)
    | Existing String String
    | YYYY Int
    | YY Int
    | MMMM Month
    | MM Month
    | Weekday DayOfWeek
    | DoM Int
    | Hour Int
    | Minute Int
    | Second Int
    | DateFrom String POSIXTime
    deriving (Generic, Show)

dowToString :: DayOfWeek -> String
dowToString dow = case dow of
    Monday -> "Mon"
    Tuesday -> "Tue"
    Wednesday -> "Wed"
    Thursday -> "Thu"
    Friday -> "Fri"
    Saturday -> "Sat"
    Sunday -> "Sun"

monthToString :: Month -> String
monthToString month =
    case month of
        MkMonth 1 -> "Jan"
        MkMonth 2 -> "Feb"
        MkMonth 3 -> "Mar"
        MkMonth 4 -> "Apr"
        MkMonth 5 -> "May"
        MkMonth 6 -> "Jun"
        MkMonth 7 -> "Jul"
        MkMonth 8 -> "Aug"
        MkMonth 9 -> "Sep"
        MkMonth 10 -> "Oct"
        MkMonth 11 -> "Nov"
        MkMonth 12 -> "Dec"
        _ -> "N/A"

posixToMillis :: POSIXTime -> Integer
posixToMillis = diffTimeToPicoseconds . realToFrac

instance ToJSON Fragment where
    toJSON f = toJSON $ case f of
        Free s -> object ["type" .= String "Free", "value" .= String (T.pack s)]
        Fixed s -> object ["type" .= String "Fixed", "value" .= String (T.pack s)]
        Sequence name padding step start value ->
            object
                [ "type" .= String "Sequence"
                , "name" .= String (T.pack name)
                , "padding" .= padding
                , "step" .= step
                , "start" .= start
                , "value" .= value
                ]
        Existing name value -> object ["type" .= String "Existing", "name" .= String (T.pack name), "value" .= String (T.pack value)]
        YYYY year -> object ["type" .= String "YYYY", "value" .= String (T.pack $ show year)]
        YY year -> object ["type" .= String "YY", "value" .= String (T.pack $ show year)]
        MMMM month -> object ["type" .= String "MMMM", "value" .= String (T.pack $ monthToString month)]
        MM month -> object ["type" .= String "MM", "value" .= String (T.pack $ monthToString month)]
        Weekday dow -> object ["type" .= String "Weekday", "value" .= String (T.pack $ dowToString dow)]
        DoM dom -> object ["type" .= String "DoM", "value" .= String (T.pack $ show dom)]
        Hour hour -> object ["type" .= String "Hour", "value" .= String (T.pack $ show hour)]
        Minute minute -> object ["type" .= String "Minute", "value" .= String (T.pack $ show minute)]
        Second second -> object ["type" .= String "Second", "value" .= String (T.pack $ show second)]
        DateFrom name posix -> object ["type" .= String "DateFrom", "name" .= String (T.pack name), "value" .= String (T.pack $ show $ posixToMillis posix)]

instance FromJSON Fragment where
    parseJSON = withObject "Fragment" $ \o -> do
        type_ <- o .: "type"
        case type_ of
            String "Free" -> Free <$> o .: "value"
            String "Fixed" -> Fixed <$> o .: "value"
            String "Sequence" -> Sequence <$> o .: "name" <*> o .: "padding" <*> o .: "step" <*> o .: "start" <*> o .: "value"
            String "Existing" -> Existing <$> o .: "name" <*> o .: "value"
            String "YYYY" -> YYYY <$> o .: "value"
            String "YY" -> YY <$> o .: "value"
            String "MMMM" -> MMMM <$> o .: "value"
            String "MM" -> MM <$> o .: "value"
            String "Weekday" -> Weekday <$> o .: "value"
            String "DoM" -> DoM <$> o .: "value"
            String "Hour" -> Hour <$> o .: "value"
            String "Minute" -> Minute <$> o .: "value"
            String "Second" -> Second <$> o .: "value"
            String "DateFrom" -> DateFrom <$> o .: "name" <*> o .: "value"
            _ -> parseFail "Unknown fragment"

isType :: T.Text -> Message -> Bool
isType t ev = getString "what" ev == Just t

excludeType :: T.Text -> [Message] -> [Message]
excludeType t = filter (not . isType t)

isAfter :: Time -> Message -> Bool
isAfter t ev =
    case ev of
        (JSON.Object o) -> case KeyMap.lookup "meta" o of
            Just m -> case getInt "posixtime" m of
                Just et -> et >= t
                Nothing -> False
            _ -> False
        _ -> False

getInt :: Key -> Message -> Maybe Int
getInt k e =
    case e of
        (JSON.Object o) -> case KeyMap.lookup k o of
            Just (JSON.Number n) -> Just n >>= toBoundedInteger
            _ -> Nothing
        _ -> Nothing

getMetaString :: Key -> Message -> Maybe T.Text
getMetaString k e =
    case e of
        (JSON.Object o) -> case KeyMap.lookup "meta" o of
            (Just (JSON.Object m)) -> case KeyMap.lookup k m of
                Just (JSON.String s) -> Just s
                _ -> Nothing
            _ -> Nothing
        _ -> Nothing

isProcessed :: Message -> Bool
isProcessed msg = maybe False ((==) "Processed" . T.unpack) (getMetaString "flow" msg)

getString :: Key -> Message -> Maybe T.Text
getString k e =
    case e of
        (JSON.Object o) -> case KeyMap.lookup k o of
            Just (JSON.String s) -> Just s
            _ -> Nothing
        _ -> Nothing

getUuids :: Message -> [Uuid]
getUuids e =
    case e of
        (JSON.Object o) -> case KeyMap.lookup "load" o of
            (Just (JSON.Object m)) ->
                case KeyMap.lookup "uuids" m of
                    Just (JSON.Array uuids) ->
                        Vector.toList
                            ( fmap
                                ( \case
                                    JSON.String uuid -> T.unpack uuid
                                    _ -> ""
                                )
                                uuids
                            )
                    _ -> []
            _ -> []
        _ -> []

setFragments :: [Fragment] -> Message -> Message
setFragments fragments message =
    case message of
        JSON.Object keymap ->
            JSON.toJSON $
                alterF
                    ( \case
                        (Just (JSON.Object m)) ->
                            Just $
                                Just $
                                    JSON.toJSON $
                                        alterF
                                            ( \case
                                                (Just _) -> Just $ Just $ JSON.Array $ Vector.fromList $ fmap toJSON fragments
                                                _ -> Nothing
                                            )
                                            "fragments"
                                            m
                        _ -> Nothing
                    )
                    "load"
                    keymap
        _ -> message

setProcessed :: Message -> Message
setProcessed e =
    case e of
        JSON.Object keymap ->
            JSON.toJSON $
                alterF
                    ( \case
                        (Just (JSON.Object m)) ->
                            Just $
                                Just $
                                    JSON.toJSON $
                                        alterF
                                            ( \case
                                                (Just _) -> Just $ Just $ JSON.String "Processed"
                                                _ -> Nothing
                                            )
                                            "flow"
                                            m
                        _ -> Nothing
                    )
                    "meta"
                    keymap
        _ -> e

getFragments :: Message -> [Fragment]
getFragments msg =
    case msg of
        (JSON.Object o) -> case KeyMap.lookup "load" o of
            (Just (JSON.Object m)) ->
                case KeyMap.lookup "fragments" m of
                    Just (JSON.Array fragments) -> do
                        result <- fromJSON <$> Vector.toList fragments
                        case result of
                            Success f -> [f]
                            Error _ -> []
                    _ -> []
            _ -> []
        _ -> []

--    case makeObj
--        [ ("uuid", JSString $ toJSString $ show uuid)
--        , ("type", JSString $ toJSString "AckReceived")
--        , ("posixtime", showJSON time)
--        , ("origin", JSString $ toJSString $ origin)
--        ] of
--        JSObject o -> o

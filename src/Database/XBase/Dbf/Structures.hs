{-# LANGUAGE RecordWildCards, EmptyDataDecls #-}
module Database.XBase.Dbf.Structures 
    ( DbfDate(..), DbfFieldDescriptor(..), DbfRecord(..)
    , DbfDatabaseContainer{- (..) -}, DbfFileHeader(..)
    , putDbfShortDate, getDbfShortDate
    , putDbfFieldName, getDbfFieldName
    , putDbfFieldDescriptor, getDbfFieldDescriptor
    , putDbfFileHeader, getDbfFileHeader
    , putDbfRecord, getDbfRecord
    , putDbfFile, getDbfFile
    ) where

import Database.XBase.Dbf.Year8

import Data.Binary.Put
import Data.Binary.Get
import Data.Word
import qualified Data.ByteString.Lazy as BS
import Control.Monad.Loops
import Data.Maybe

putFlag True  = putWord8 1
putFlag False = putWord8 0
getFlag = do
    f <- getWord8
    return (f /= 0)

data DbfDate yearType = DbfDate
    { dbfYear   :: yearType
    , dbfMonth  :: Word8
    , dbfDay    :: Word8
    } deriving (Eq, Show)

putDbfShortDate DbfDate {..} = do
    putYear8 dbfYear
    putWord8 dbfMonth
    putWord8 dbfDay
    {- 3 bytes total -}

getDbfShortDate = do
    dbfYear     <- getYear8
    dbfMonth    <- getWord8
    dbfDay      <- getWord8
    return (DbfDate dbfYear dbfMonth dbfDay)
    {- 3 bytes total -}

data DbfFieldDescriptor = DbfFieldDescriptor
    { dbfFieldName          :: BS.ByteString {- max length (10? 11?) -}
    , dbfFieldType          :: Word8
    , dbfFieldAddress       :: Word32 {- mem addr for DBase, offset in record for foxpro. -}
    , dbfFieldLength        :: Word8
    , dbfFieldDecimals      :: Word8
    , dbfFieldWorkArea      :: Word8
    , dbfFieldSetFieldsFlag :: Bool
    , dbfFieldIndexedFlag   :: Bool
    } deriving (Eq, Show)

dbfRecLengthForFields fields = 1 + sum (map dbfFieldLength fields)

putDbfFieldName bs 
    | len > maxLen  = fail ("putDbfFieldName: Field name too long (" ++ show len ++ " bytes)")
    | otherwise = do
        putLazyByteString bs
        putLazyByteString (BS.replicate (maxLen-len) 0)
        {- 11 bytes total -}
    where
        maxLen = 11
        len = BS.length bs

getDbfFieldName = getLazyByteString 11

putDbfFieldDescriptor DbfFieldDescriptor {..} = do
    {-  0: Field Name -}            putDbfFieldName dbfFieldName
    {- 11: Field Type -}            putWord8 dbfFieldType
    {- 12: Field Addr -}            putWord32le dbfFieldAddress
    {- 16: Field Length -}          putWord8 dbfFieldLength
    {- 17: Decimal Count -}         putWord8 dbfFieldDecimals
    {- 18: Reserved (2 bytes) -}    putWord16le 0
    {- 20: Work Area ID -}          putWord8 dbfFieldWorkArea
    {- 21: Reserved (2 bytes) -}    putWord16le 0
    {- 23: Flag for SET FIELDS -}   putFlag dbfFieldSetFieldsFlag
    {- 24: Reserved (7 bytes) -}    putLazyByteString (BS.replicate 7 0)
    {- 31: Index Field Flag -}      putFlag dbfFieldIndexedFlag
    {- 32 bytes total -}

getDbfFieldDescriptor = do
    {-  0: Field Name -}            dbfFieldName            <- getDbfFieldName
    {- 11: Field Type -}            dbfFieldType            <- getWord8
    {- 12: Field Addr -}            dbfFieldAddress         <- getWord32le
    {- 16: Field Length -}          dbfFieldLength          <- getWord8
    {- 17: Decimal Count -}         dbfFieldDecimals        <- getWord8
    {- 18: Reserved (2 bytes) -}    getWord16le             -- discarding result
    {- 20: Work Area ID -}          dbfFieldWorkArea        <- getWord8
    {- 21: Reserved (2 bytes) -}    getWord16le             -- discarding result
    {- 23: Flag for SET FIELDS -}   dbfFieldSetFieldsFlag   <- getFlag
    {- 24: Reserved (7 bytes) -}    getLazyByteString 7     -- discarding result
    {- 31: Index Field Flag -}      dbfFieldIndexedFlag     <- getFlag
    {- 32 bytes total -}            return DbfFieldDescriptor
                                        { dbfFieldName          = dbfFieldName
                                        , dbfFieldType          = dbfFieldType
                                        , dbfFieldAddress       = dbfFieldAddress
                                        , dbfFieldLength        = dbfFieldLength
                                        , dbfFieldDecimals      = dbfFieldDecimals
                                        , dbfFieldWorkArea      = dbfFieldWorkArea
                                        , dbfFieldSetFieldsFlag = dbfFieldSetFieldsFlag
                                        , dbfFieldIndexedFlag   = dbfFieldIndexedFlag
                                        }

data DbfDatabaseContainer -- not implemented
instance Eq DbfDatabaseContainer
instance Show DbfDatabaseContainer

data DbfFileHeader = DbfFileHeader
    { dbfFileSignature      :: Word8
    , dbfFileUpdateDate     :: DbfDate Year8
    , dbfFileNumRecords     :: Word32
    , dbfFileHdrLength      :: Word16
    , dbfFileRecLength      :: Word16
    , dbfFileTxInc          :: Bool
    , dbfFileEncr           :: Bool
    , dbfFileMDX            :: Bool {- ?? -}
    , dbfFileLangCode       :: Word8
    , dbfFileFields         :: [DbfFieldDescriptor]
    , dbfFileDbContainer    :: Maybe DbfDatabaseContainer
    } deriving (Eq, Show)

putDbfFileHeader DbfFileHeader {..} = do
    {-  0: Signature -}             putWord8 dbfFileSignature
    {-  1: Date of last update -}   putDbfShortDate dbfFileUpdateDate
    {-  4: Number of records -}     putWord32le dbfFileNumRecords
    {-  8: Length of header -}      putWord16le dbfFileHdrLength
    {- 10: Length of each record -} putWord16le dbfFileRecLength
    {- 12: reserved (2 bytes) -}    putWord16le 0
    {- 14: Incomplete TX -}         putFlag dbfFileTxInc
    {- 15: Encryption flag -}       putFlag dbfFileEncr
    {- 16: Free Rec Thread (n/i)-}  putWord32le 0
    {- 20: Reserved (8 bytes) -}    putWord64le 0
    {- 28: MDX flag -}              putFlag dbfFileMDX
    {- 29: Language Driver -}       putWord8 dbfFileLangCode
    {- 30: Reserved (2 bytes) -}    putWord16le 0
    {- 32: Field Descriptors -}     mapM_ putDbfFieldDescriptor dbfFileFields
    {- _: Terminator (0x0d) -}      putWord8 0x0d
    {- _: Database Container -}     -- Not Implemented

getDbfFileHeader = do               start <- bytesRead
    {-  0: Signature -}             dbfFileSignature    <- getWord8
    {-  1: Date of last update -}   dbfFileUpdateDate   <- getDbfShortDate
    {-  4: Number of records -}     dbfFileNumRecords   <- getWord32le 
    {-  8: Length of header -}      dbfFileHdrLength    <- getWord16le 
    {- 10: Length of each record -} dbfFileRecLength    <- getWord16le 
    {- 12: reserved (2 bytes) -}    getWord16le         -- discarding result
    {- 14: Incomplete TX -}         dbfFileTxInc        <- getFlag
    {- 15: Encryption flag -}       dbfFileEncr         <- getFlag
    {- 16: Free Rec Thread (n/i)-}  getWord32le         -- discarding result
    {- 20: Reserved (8 bytes) -}    getWord64le         -- discarding result
    {- 28: MDX flag -}              dbfFileMDX          <- getFlag
    {- 29: Language Driver -}       dbfFileLangCode     <- getWord8
    {- 30: Reserved (2 bytes) -}    getWord16le         -- discarding result
    {- 32: Field Descriptors -}     let notDone = do
                                            x <- lookAhead getWord8
                                            return (x /= 0x0d)
                                    dbfFileFields <- whileM notDone getDbfFieldDescriptor
    {- _: Terminator (0x0d) -}      0x0d <- getWord8    -- should not fail!
    {- _: Database Container -}     -- Not Implemented
    {- read to end and discard -}   here <- bytesRead
                                    let consumed = here - start
                                    skip (fromIntegral dbfFileHdrLength - fromIntegral consumed)
                                    return DbfFileHeader
                                        { dbfFileSignature      = dbfFileSignature
                                        , dbfFileUpdateDate     = dbfFileUpdateDate
                                        , dbfFileNumRecords     = dbfFileNumRecords
                                        , dbfFileHdrLength      = dbfFileHdrLength
                                        , dbfFileRecLength      = dbfFileRecLength
                                        , dbfFileTxInc          = dbfFileTxInc
                                        , dbfFileEncr           = dbfFileEncr
                                        , dbfFileMDX            = dbfFileMDX
                                        , dbfFileLangCode       = dbfFileLangCode
                                        , dbfFileFields         = dbfFileFields
                                        , dbfFileDbContainer    = Nothing
                                        }

data DbfRecord = DbfRecord
    { dbfRecDeleted         :: Bool
    , dbfRecData            :: BS.ByteString
    } deriving (Eq, Show)

dbfRecordDeleted, dbfRecordNotDeleted, dbfRecordEOF :: Word8
dbfRecordDeleted = 0x2A
dbfRecordNotDeleted = 0x20
dbfRecordEOF = 0x1A

putDbfRecord DbfRecord{..} = do
    {- 0: Deleted -}    putWord8 $ if dbfRecDeleted then 0x2A else 0x20
    {- 1: Data -}       putLazyByteString dbfRecData

getDbfRecord recLen = do
    {- 0: tag -}    tag <- getWord8
                    deleted <- case tag of
                        0x2A {-deleted-}    -> return (Just True)
                        0x20 {-valid-}      -> return (Just False)
                        0x1A {-EOF-}        -> return Nothing
                        _ {-defective dbf-} -> fail "getDbfRecord: corrupt or non-dbf file (invalid record marker found)"
                    whenJust deleted $ \deleted -> do
                            dat <- getLazyByteString (recLen - 1)
                            return (Just (DbfRecord deleted dat))

whenJust Nothing  f = return Nothing
whenJust (Just x) f = f x

putDbfFile hdr recs = do
    putDbfFileHeader hdr
    mapM_ putDbfRecord recs
    putWord8 dbfRecordEOF

getDbfFile :: Get (DbfFileHeader, [DbfRecord])
getDbfFile = do
    hdr <- getDbfFileHeader
    recs <- unfoldM (getDbfRecord (fromIntegral (dbfFileRecLength hdr)))
    return (hdr, recs)
    
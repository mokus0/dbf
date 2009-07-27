{-# LANGUAGE RecordWildCards #-}
module Database.XBase.Dbf.Handle
    ( DbfHandle
    , openDbf
    , closeDbf
    , dbfIsOpen
    , dbfHeader
    
    , DbfFieldHandle
    , fieldNum
    , fieldName
    , fieldDesc
    
    , dbfNumFields
    , dbfFields
    , dbfGetField
    , dbfLookupField
    
    , DbfRecHandle
    , recNum
    
    , dbfRecords
    , dbfNumRecords
    , dbfGetRecord
    
    , readDbfField
    
    ) where

import Database.XBase.Dbf.Structures

import System.IO
import Control.Concurrent.RWLock
import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Lazy.Char8 as BSC
import Data.Binary.Get

data DbfHandle = DbfHandle
    { dbfReadOnly   :: Bool
    , dbfLock       :: RWLock
    , dbfFile       :: Handle
    }

instance Eq DbfHandle where
    h1 == h2    =  dbfFile h1 == dbfFile h2

withDbfFile_ :: DbfHandle -> IOMode -> (Handle -> IO a) -> IO a
withDbfFile_ DbfHandle{..} mode action = withLock dbfLock (action dbfFile)
    where withLock = case mode of
            ReadMode    -> withReadLock
            _           -> withWriteLock

withDbfFile :: DbfHandle -> IOMode -> (Handle -> IO a) -> IO a
withDbfFile dbf@DbfHandle{..} mode action = case (mode, dbfReadOnly) of
    (ReadMode, _)   -> allow
    (_, False)      -> allow
    (_, True)       -> deny
    where
        allow = withDbfFile_ dbf mode action
        deny  = fail "withDbfFile: write attempted on dbf which was opened as read-only"
    
readDbfBlock :: DbfHandle -> Integer -> Int -> IO BS.ByteString
readDbfBlock dbf pos len = withDbfFile dbf ReadMode $ \file -> do
    hSeek file AbsoluteSeek pos
    BS.hGet file len

writeDbfBlock :: DbfHandle -> Integer -> BS.ByteString -> IO ()
writeDbfBlock dbf pos dat = withDbfFile dbf WriteMode $ \file -> do
    hSeek file AbsoluteSeek pos
    BS.hPut file dat

data DbfFieldHandle = DbfFieldHandle
    { fieldDbf          :: DbfHandle
    , fieldNum          :: !Int
    , fieldOffset       :: !Int
    , fieldDesc         :: DbfFieldDescriptor
    }

instance Eq DbfFieldHandle where
    f1 == f2    =  fieldDbf f1 == fieldDbf f2
                && fieldNum f1 == fieldNum f2

mkField dbf n off field = DbfFieldHandle
    { fieldDbf          = dbf
    , fieldNum          = n
    , fieldOffset       = off
    , fieldDesc         = field
    }

fieldName :: DbfFieldHandle -> String
fieldName DbfFieldHandle{..} = BSC.unpack rawName
    where rawName = BS.takeWhile (/= 0) (dbfFieldName fieldDesc)

fieldDescrOff :: Int -> Integer
fieldDescrOff n = toInteger (0x20 + 0x20*n)

fieldLength :: DbfFieldHandle -> Int
fieldLength = fromIntegral . dbfFieldLength . fieldDesc

data DbfRecHandle = DbfRecHandle
    { recDbf        :: DbfHandle
    , recNum        :: !Int
    , recOffset     :: Int
    } deriving Eq

mkRec dbf n off = DbfRecHandle dbf n off

openDbf :: FilePath -> Bool -> IO DbfHandle
openDbf file dbfReadOnly = do
    let mode    | dbfReadOnly   = ReadMode
                | otherwise     = ReadWriteMode
    dbfFile <- openBinaryFile file mode
    
    dbfLock     <- newRWLockIO
    return DbfHandle
        { dbfReadOnly   = dbfReadOnly
        , dbfLock       = dbfLock
        , dbfFile       = dbfFile
        }

dbfIsOpen :: DbfHandle -> IO Bool
dbfIsOpen DbfHandle{..} = hIsOpen dbfFile

closeDbf :: DbfHandle -> IO ()
closeDbf dbf = withDbfFile_ dbf WriteMode hClose

getHeaderSize :: DbfHandle -> IO Int
getHeaderSize dbf = do
    sz <- readDbfBlock dbf 8 2
    return (fromIntegral (runGet getWord16le sz))

getRecordSize :: DbfHandle -> IO Int
getRecordSize dbf = do
    sz <- readDbfBlock dbf 10 2
    return (fromIntegral (runGet getWord16le sz))

dbfHeader :: DbfHandle -> IO DbfFileHeader
dbfHeader dbf = do
    sz <- getHeaderSize dbf
    hdr <- readDbfBlock dbf 0 sz
    return (runGet getDbfFileHeader hdr)

dbfNumFields :: DbfHandle -> IO Int
dbfNumFields = fmap length . dbfFields
dbfGetField :: DbfHandle -> Int -> IO DbfFieldHandle
dbfGetField dbf n = fmap (!!n) (dbfFields dbf)

dbfFields :: DbfHandle -> IO [DbfFieldHandle]
dbfFields dbf = go id 0 1
    where 
        go f n off = do
            fld <- getField dbf n
            case fld of
                Nothing -> return (f [])
                Just fld -> 
                    let sz = fromIntegral (dbfFieldLength fld)
                     in go (f . (mkField dbf n off fld:)) (succ n) (off + sz)

dbfLookupField :: DbfHandle -> String -> IO (Maybe DbfFieldHandle)
dbfLookupField dbf fName = lookupField dbf ((==fName) . fieldName)
    
getField dbf n = do
    let off = fieldDescrOff n
    fld <- readDbfBlock dbf off 1
    if fld `BS.index` 0 == 0x0D
        then return Nothing
        else do
            fld <- readDbfBlock dbf off 0x20
            return (Just (runGet getDbfFieldDescriptor fld))

lookupField dbf p = go 0 1
    where go n off = do
            fd <- getField dbf n
            case fd of
                Nothing -> return Nothing
                Just fd -> let sz = fromIntegral (dbfFieldLength fd)
                               field = mkField dbf n off fd
                            in if p field then return (Just field)
                                          else go (succ n) (off+sz)

    
dbfRecords :: DbfHandle -> IO [DbfRecHandle]
dbfRecords dbf = do
    start <- getHeaderSize dbf
    stride <- getRecordSize dbf
    
    let go f n = do
            let pos = start + n * stride
            rec <- readDbfBlock dbf (toInteger pos) 1
            case rec `BS.index` 0 of
                0x2A {-deleted-}    -> go f $! succ n
                0x20 {-valid-}      -> go (f . (mkRec dbf n pos:)) $! succ n
                0x1A {-EOF-}        -> return (f [])
                _ {-defective dbf-} -> fail "dbfRecords: corrupt or non-dbf file (invalid record marker found)"
    
    go id 0

dbfNumRecords :: DbfHandle -> IO Integer
dbfNumRecords dbf = do
    n <- readDbfBlock dbf 4 4
    return (toInteger (runGet getWord32le n))
    
dbfGetRecord :: DbfHandle -> Integer -> IO (Maybe DbfRecHandle)
dbfGetRecord dbf n = do
    nRecs <- dbfNumRecords dbf
    if n >= nRecs
        then return Nothing
        else do
            start <- getHeaderSize dbf
            stride <- getRecordSize dbf
    
            let pos = toInteger start + n * toInteger stride
            rec <- readDbfBlock dbf (toInteger pos) 1
            if rec `BS.index` 0 == 0x20
                then return (Just (mkRec dbf (safeFromInteger n) (safeFromInteger pos)))
                else return Nothing

-- BS.hGet uses "Int" offset, which could be very bad if dealing with large
-- databases.  That's not a big enough problem to make me feel like actually
-- fixing it, but it should at least "barf gracefully" if someone tries it.
safeFromInteger :: (Bounded b, Integral b) => Integer -> b
safeFromInteger n = x
    where
        mn = toInteger (minBound `asTypeOf` x)
        mx = toInteger (maxBound `asTypeOf` x)
        x   | n < mn || n > mx
            = error ("safeFromInteger: integral value out of range: " ++ show n)
            | otherwise
            = fromInteger n

readDbfField :: DbfRecHandle -> DbfFieldHandle -> IO BS.ByteString
readDbfField DbfRecHandle{..} DbfFieldHandle{..}
    | recDbf /= fieldDbf = fail "readDbfField: Record and field are from different DbfHandles"
    | otherwise = do
        let dbf = recDbf
            pos = recOffset
            off = fieldOffset
            len = fromIntegral (dbfFieldLength fieldDesc)
        
        readDbfBlock dbf (toInteger (pos+off)) len

writeDbfField :: DbfRecHandle -> DbfFieldHandle -> BS.ByteString -> IO ()
writeDbfField DbfRecHandle{..} fld@DbfFieldHandle{..} value
    | recDbf /= fieldDbf
    = fail "writeDbfField: Record and field are from different DbfHandles"
    | dataLen > fieldLen
    = fail ("writeDbfField: Value supplied is larger than target field (" ++ show value ++ " -> " ++ fieldName fld ++ ")")
    | otherwise = do
        let pos = recOffset
            off = fieldOffset
            padded 
                | fieldLen == dataLen 
                = value
                | otherwise -- fieldLen > dataLen
                = value `BS.append` BS.replicate (fieldLen - dataLen) 0x20
        
        writeDbfBlock recDbf (toInteger (pos + off)) padded

    where 
        dataLen  = BS.length value
        fieldLen = fromIntegral (dbfFieldLength fieldDesc)

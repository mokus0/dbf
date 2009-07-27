module Database.XBase.Dbf
    ( module Database.XBase.Dbf.Structures
    , module Database.XBase.Dbf.Handle
    , module Database.XBase.Dbf.Year8
    , readDbfFile, writeDbfFile
    ) where

import Database.XBase.Dbf.Structures
import Database.XBase.Dbf.Handle
import Database.XBase.Dbf.Year8

import Data.Binary.Get
import Data.Binary.Put

import qualified Data.ByteString.Lazy as BS

readDbfFile path = do
    file <- BS.readFile path
    return (runGet getDbfFile file)

writeDbfFile path dbf = do
    BS.writeFile path (runPut (uncurry putDbfFile dbf))


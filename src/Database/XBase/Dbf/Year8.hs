module Database.XBase.Dbf.Year8
    ( Year8
    , putYear8
    , getYear8
    ) where

import Data.Binary

-- A custom "8-bit year" integral type whose values range from 1900 to 2155.
-- For obvious reasons, the arithmetic function implementations are
-- more or less useless, but they aren't the point.
newtype Year8 = Y19xx Word8
    deriving (Eq, Ord, Bounded)

instance Enum Year8 where
    toEnum x
        |  x < 1900
        || x > 1900+255
        = error ("Year8: value out of range: " ++ show x)
        
        | otherwise = Y19xx (toEnum (x - 1900))
    fromEnum (Y19xx x) = fromEnum x + 1900

instance Show Year8 where
    showsPrec p = showsPrec p . fromEnum

instance Read Year8 where
    readsPrec p str = 
        [ (toEnum x, ys)
        | (x, ys) <- readsPrec p str
        ]

liftY8 (*) a b = fromInteger (toInteger a * toInteger b)
instance Num Year8 where
    (+) = liftY8 (+)
    (-) = liftY8 (-)
    (*) = liftY8 (*)
    negate = toEnum . negate . fromEnum
    abs = id
    signum x = x `seq` 1
    
    -- only really want to be able to use 'fromInteger'
    fromInteger x
        |  x < 1900
        || x > 1900+255
        = error ("Year8: value out of range: " ++ show x)
        
        | otherwise = Y19xx (fromInteger (x - 1900))

instance Real Year8 where
    toRational = toRational . fromEnum

instance Integral Year8 where
    quotRem p q = case quotRem (toInteger p) (toInteger q) of
        (q,r) -> (fromInteger q, fromInteger r)
    divMod p q = case divMod (toInteger p) (toInteger q) of
        (q,r) -> (fromInteger q, fromInteger r)
    -- only really want to be able to pattern-match
    toInteger (Y19xx x) = toInteger x + 1900

putYear8 (Y19xx x) = putWord8 x
getYear8 = fmap Y19xx getWord8

instance Binary Year8 where
    get = getYear8
    put = putYear8
{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module ZKVar
  ( ZKVar(..)
  , ZKHandle
  , children
  , exists
  , initZK
  , mkdir
  , mkdir_p
  , newEmptyZKVar
  , putZKVar
  , rm
  , rm_r
  , takeZKVar
  , takeZKVarOr
  , tryTakeZKVar
  , withZK
  ) where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Data.ByteString.Char8
import Data.Maybe

import qualified Control.Exception as E
import qualified Data.List         as L
import qualified Zookeeper.Core    as ZK
import qualified Zookeeper.Utils   as ZKU

type Path = String
type ZKHandle = ZK.Handle

data ZKVar a = ZKVar ZKHandle Path

initZK :: String -> IO ZKHandle
initZK host = ZK.init host Nothing 10000 Nothing

withZK :: String -> (ZKHandle -> IO a) -> IO a
withZK = ZKU.withHandle

newEmptyZKVar :: ZKHandle -> Path -> ZKVar a
newEmptyZKVar = ZKVar

takeZKVar :: Read a => ZKVar a -> IO a
takeZKVar z@(ZKVar zk p) = do
  m <- newEmptyMVar
  v <- ZK.exists zk p $ Just $ \_ _ _ _ -> unsafeGet z >>= putMVar m
  maybe (takeMVar m) (\_ -> unsafeGet z) v

tryTakeZKVar :: Read a => ZKVar a -> IO (Maybe a)
tryTakeZKVar z = exists z >>= maybe (return Nothing)
                                    (\_ -> Just <$> unsafeGet z)

takeZKVarOr :: Read a => ZKVar a -> a -> IO a
takeZKVarOr z defv = fromMaybe defv <$> tryTakeZKVar z

putZKVar :: Show a => ZKVar a -> a -> [ZK.CreateFlag] -> IO (ZKVar a)
putZKVar z@(ZKVar zk p) v fs =
  exists z >>= maybe (mkdirp >>= \z' -> putZKVar z' v fs)
                     (\_ -> set >> return z)
  where
    set    = ZK.set zk p (show v) Nothing
    mkdirp = fmap L.head $ mkdir_p z fs

exists :: ZKVar a -> IO (Maybe ZK.Stat)
exists (ZKVar zk p) = ZK.exists zk p Nothing

-- | Create the node on the ZK server (without data). Throws on error, most
-- notably if the node already exists or the parent node does _not_ yet exist.
mkdir :: ZKVar a -> [ZK.CreateFlag] -> IO (ZKVar a)
mkdir z@(ZKVar zk p) fs = ZK.create zk p [] fs ZK.openAclUnsafe >> return z

-- | Create the node and intermediate nodes as required. Won't throw if any of
-- the nodes already exist, but other exceptions are propagated. The returned
-- list reflects the path up to the given node, in reverse order (eg.
-- ["/a/b/c", "/a/b", "/a", "/"])
mkdir_p :: ZKVar a -> [ZK.CreateFlag] -> IO [ZKVar a]
mkdir_p (ZKVar zk p) fs = do
  let ps = L.map (newEmptyZKVar zk . unpack . cons '/' . intercalate "/")
                 (L.inits . L.tail . split '/' . pack $ p)
      ms = fs : L.replicate (L.length ps) []
  zs <- mapM mkdir' . L.zip ps $ ms
  return $ L.reverse zs
  where
    mkdir' (zkvar, mode) = E.catch (mkdir zkvar mode)
                                   (\(_ :: ZK.NodeExists) -> return zkvar)

-- | Get the node's children
children :: ZKVar a -> IO [ZKVar a]
children (ZKVar zk p) = do
  cs <- ZK.getChildren zk p Nothing
  return $ L.map (newEmptyZKVar zk . (parent_path ++)) cs
  where
    parent_path = p ++ "/"

-- | Remove the node. Throws if the node has children (see @@rm_r@@)
rm :: ZKVar a -> IO (ZKVar a)
rm z@(ZKVar zk p) = do
  exists z >>= maybe (ZK.delete zk p $ Just (-1))
                     (\_ -> return ())
  return z

-- | Remove the node recursively
rm_r :: ZKVar a -> IO (ZKVar a)
rm_r z = children z >>= mapM rm_r >> rm z

unsafeGet :: Read a => ZKVar a -> IO a
unsafeGet (ZKVar zk p) = ZK.get zk p Nothing >>= \(v,_) -> (return . read) v

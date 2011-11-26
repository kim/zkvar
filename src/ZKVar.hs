{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module ZKVar
  ( -- * manipulate ZKVar
    ZKVar(..)
  , children
  , exists
  , mkdir
  , mkdir_p
  , new
  , put
  , rm
  , rm_r
  , take
  , takeOr
  , tryTake
  , watch
  -- * manage ZK handle
  , ZKHandle
  , initZK
  , releaseZK
  , withZK
  ) where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Data.ByteString.Char8 hiding (take)
import Data.Maybe
import Prelude hiding (take)

import qualified Control.Exception as E
import qualified Data.List         as L
import qualified Zookeeper.Core    as ZK
import qualified Zookeeper.Utils   as ZKU

type Path = String
type ZKHandle = ZK.Handle

data ZKVar a = ZKVar ZKHandle Path

initZK :: String -> IO ZKHandle
initZK host = ZK.init host Nothing 10000 Nothing

releaseZK :: ZKHandle -> IO ()
releaseZK = ZK.close

withZK :: String -> (ZKHandle -> IO a) -> IO a
withZK = ZKU.withHandle

new :: ZKHandle -> Path -> ZKVar a
new = ZKVar

take :: Read a => ZKVar a -> IO a
take z@(ZKVar zk p) = do
  m <- newEmptyMVar
  v <- ZK.exists zk p $ Just $ \_ _ _ _ -> unsafeGet z >>= putMVar m
  maybe (takeMVar m) (\_ -> unsafeGet z) v

tryTake :: Read a => ZKVar a -> IO (Maybe a)
tryTake z = exists z >>= maybe (return Nothing) (\_ -> Just <$> unsafeGet z)

takeOr :: Read a => ZKVar a -> a -> IO a
takeOr z defv = fromMaybe defv <$> tryTake z

put :: Show a => ZKVar a -> a -> [ZK.CreateFlag] -> IO (ZKVar a)
put z@(ZKVar zk p) v fs =
  exists z >>= maybe (mkdirp >>= \z' -> put z' v fs)
                     (\_ -> set >> return z)
  where
    set    = ZK.set zk p (show v) Nothing
    mkdirp = L.head <$> mkdir_p z fs

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
mkdir_p z fs = do
  let ps = pathTo z
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
  return $ L.map (new zk . (parent_path ++)) cs
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

-- | Watch node forever, or until it is removed. The node may not exist when
-- calling this function. The callback function is called with @@Nothing@@ if
-- the node was removed, @@Just a@@ if it was changed or created.
watch :: Read a => ZKVar a -> (Maybe a -> IO ()) -> IO ()
watch z cb = maybeChanged
  where
    maybeChanged    = getOrWatch z watcher >>= cb
    watcher _ _ _ _ = maybeChanged

-- util

unsafeGet :: Read a => ZKVar a -> IO a
unsafeGet (ZKVar zk p) = ZK.get zk p Nothing >>= \(v,_) -> (return . read) v

getOrWatch :: Read a => ZKVar a -> ZK.Watcher -> IO (Maybe a)
getOrWatch z@(ZKVar zk p) w =
  ZK.exists zk p (Just w) >>=
    maybe (return Nothing) (\_ -> Just <$> (unsafeGet z))

pathTo :: ZKVar a -> [ZKVar a]
pathTo (ZKVar zk p) =
  L.map (new zk . unpack . cons '/' . intercalate "/")
        (L.inits . L.tail . split '/' . pack $ p)

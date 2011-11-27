{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module ZKVar
  ( -- * manipulate ZKVar
    ZKVar(..)
  , children
  , exists
  , existsOrWatch
  , ifExists
  , mkdir
  , mkdir_p
  , new
  , put
  , rm
  , rm_r
  , take
  , takeOr
  , takeOrWatch
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

type Path     = String
type ZKHandle = ZK.Handle
type Watcher  = ZK.Watcher

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
take z = do
  m <- newEmptyMVar
  takeOrWatch z (watcher m) >>= maybe noop (putMVar m)
  takeMVar m
  where
    watcher m _ _ _ _ = unsafeGet z >>= putMVar m

tryTake :: Read a => ZKVar a -> IO (Maybe a)
tryTake z = tryTake' z Nothing

takeOr :: Read a => ZKVar a -> a -> IO a
takeOr z defv = fromMaybe defv <$> tryTake z

takeOrWatch :: Read a => ZKVar a -> Watcher -> IO (Maybe a)
takeOrWatch z w = tryTake' z (Just w)

put :: Show a => ZKVar a -> a -> [ZK.CreateFlag] -> IO (ZKVar a)
put z v fs = ifExists z mkdirp set >> return z
  where
    set (ZKVar zk p) = ZK.set zk p (show v) Nothing
    mkdirp = (\z' -> put z' v fs) . L.head <$> mkdir_p z fs >> noop

exists :: ZKVar a -> IO Bool
exists (ZKVar zk p) = isJust <$> ZK.exists zk p Nothing

existsOrWatch :: ZKVar a -> Watcher -> IO Bool
existsOrWatch (ZKVar zk p) w = isJust <$> ZK.exists zk p (Just w)

-- TODO: callback return types are somewhat less-than-optimal
ifExists :: ZKVar a -> IO () -> (ZKVar a -> IO ()) -> IO ()
ifExists z@(ZKVar zk p) no yes =
  ZK.exists zk p Nothing >>= maybe no (\_ -> yes z)

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
rm z = ifExists z noop rm' >> return z
  where
    rm' (ZKVar zk p) = ZK.delete zk p $ Just (-1)

-- | Remove the node recursively
rm_r :: ZKVar a -> IO (ZKVar a)
rm_r z = children z >>= mapM rm_r >> rm z

-- | Watch node forever, or until it is removed. The node may not exist when
-- calling this function. The callback function is called with @@Nothing@@ if
-- the node was removed, @@Just a@@ if it was changed or created.
watch :: Read a => ZKVar a -> (Maybe a -> IO ()) -> IO ()
watch z cb = maybeChanged
  where
    maybeChanged    = takeOrWatch z watcher >>= cb
    watcher _ _ _ _ = maybeChanged

-- util

unsafeGet :: Read a => ZKVar a -> IO a
unsafeGet (ZKVar zk p) = ZK.get zk p Nothing >>= \(v,_) -> (return . read) v

tryTake' :: Read a => ZKVar a -> Maybe Watcher -> IO (Maybe a)
tryTake' = go
  where
    go  z@(ZKVar zk p) (Just w) = ZK.exists zk p (Just w) >>= get z
    go  z@(ZKVar zk p) Nothing  = ZK.exists zk p Nothing  >>= get z
    get z = maybe (return Nothing) (\_ -> Just <$> unsafeGet z)

pathTo :: ZKVar a -> [ZKVar a]
pathTo (ZKVar zk p) =
  L.map (new zk . unpack . cons '/' . intercalate "/")
        (L.inits . L.tail . split '/' . pack $ p)

noop :: IO ()
noop = return ()

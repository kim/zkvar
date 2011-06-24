module ZKVar
  ( ZKVar(..)
  , ZKHandle
  , initZK
  , withZK
  , newEmptyZKVar
  , takeZKVar
  , tryTakeZKVar
  , takeZKVarOr
  , putZKVar
  ) where

import Control.Applicative
import Control.Concurrent.MVar
import Data.Maybe
import qualified Zookeeper.Core  as ZK
import qualified Zookeeper.Utils as ZKU

type Path = String
type ZKHandle = ZK.Handle

data ZKVar a = ZKVar ZKHandle Path

initZK :: String -> IO ZKHandle
initZK host = ZK.init host Nothing 10000 Nothing

withZK :: String -> (ZKHandle -> IO a) -> IO a
withZK host = ZKU.withHandle host

newEmptyZKVar :: ZKHandle -> Path -> ZKVar a
newEmptyZKVar = ZKVar

takeZKVar :: Read a => ZKVar a -> IO a
takeZKVar z@(ZKVar zk p) = do
  m <- newEmptyMVar
  v <- ZK.exists zk p $ Just $ \_ _ _ _ -> get z >>= putMVar m
  if isJust v then get z else takeMVar m

tryTakeZKVar :: Read a => ZKVar a -> IO (Maybe a)
tryTakeZKVar z = exists z >>= \z' ->
                 if isJust z' then Just <$> get z else return Nothing

takeZKVarOr :: Read a => ZKVar a -> a -> IO a
takeZKVarOr z defv = fromMaybe defv <$> tryTakeZKVar z

putZKVar :: Show a => ZKVar a -> a -> IO (ZKVar a)
putZKVar z@(ZKVar zk p)  v = do
  z' <- exists z
  let v' = show v
  _  <- if isJust z'
          then ZK.set zk p v' Nothing
          else ZK.create zk p v' [ZK.Ephemeral] ZK.openAclUnsafe >> return ()
  return z

-- util

exists :: ZKVar a -> IO (Maybe ZK.Stat)
exists (ZKVar zk p) = ZK.exists zk p Nothing

get :: Read a => ZKVar a -> IO a
get (ZKVar zk p) = ZK.get zk p Nothing >>= \(v,_) -> (return . read) v

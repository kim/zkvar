module ZKVar
  ( ZKVar(..)
  , ZKHandle
  , initZK
  , newZKVar
  , getZKVar
  ) where

import Control.Applicative
import Data.IORef
import qualified Zookeeper.Core as ZK

type Path = String

data ZKVar = ZKVar
  { zk :: ZK.Handle
  , path :: Path
  , defaultValue :: String
  , value :: IORef String
  }

type ZKHandle = ZK.Handle

initZK :: String -> IO ZKHandle
initZK host = ZK.init host Nothing 10000 Nothing

newZKVar :: ZKHandle -> Path -> String -> IO ZKVar
newZKVar zh p defv = zkvar <$> getOr' defv
  where
    zkvar    = ZKVar zh p defv
    getOr' x = do
      ref <- newIORef defv
      val <- getOr zh p ref x
      writeIORef ref val
      return ref

getZKVar :: ZKVar -> IO String
getZKVar = readIORef . value

getOr :: ZKHandle -> Path -> IORef String -> String -> IO String
getOr zh p ref defv = exists >>= maybe (return defv) (\_ -> get)
  where
    watch' = Just (watch defv ref)
    exists = ZK.exists zh p watch'
    get    = ZK.get zh p watch' >>= \(v,_) -> return v

watch :: String -> IORef String -> ZK.Watcher
watch defv v zh = handle
  where
    handle _ _ = update
    update p = getOr zh p v defv >>= \x -> atomicModifyIORef v (\_ -> (x, ()))

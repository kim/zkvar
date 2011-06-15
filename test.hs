import Control.Monad (forever)
import ZKVar
import qualified Zookeeper.Core as ZK

main = do
  zk  <- ZK.init "127.0.0.1:2181" Nothing 10000 Nothing
  zv1 <- newZKVar zk "/foo" "default foo"
  zv2 <- newZKVar zk "/bar" "default bar"
  forever $ do
    is <- getLine
    getZKVar zv1 >>= print
    getZKVar zv2 >>= print


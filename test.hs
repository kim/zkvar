import Control.Monad (forever)
import ZKVar

main = do
  zk  <- initZK "127.0.0.1:2181"
  zv1 <- newZKVar zk "/foo" "default foo"
  zv2 <- newZKVar zk "/bar" "default bar"
  forever $ do
    is <- getLine
    getZKVar zv1 >>= print
    getZKVar zv2 >>= print


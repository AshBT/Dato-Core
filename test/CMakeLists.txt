project(GraphLabTest)

# subdirs must be in dependency order
subdirs(
  util
  serialization
  logger
  timer
  random
  fault
  fileio
  cppipc
  flexible_type
  parallel
  fiber
  rpc
  zookeeper_util
  lambda
  sframe
  sgraph
  lazy_eval
  unity
  sketches
  )

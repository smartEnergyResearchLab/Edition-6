# Function by Steve Weston on Stack Overflow page 
# "un-register a doParallel cluster
# 
# If a cluster is used the regression requires this function



# to overcome an error - just needed if we used parallel before
unregister_dopar <- function() {
  env <- foreach:::.foreachGlobals
  rm(list = ls(name = env), pos = env)
}
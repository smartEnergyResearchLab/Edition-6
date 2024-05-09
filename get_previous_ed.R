

get_previous_ed <- function(ed) {
  # ed should be of format '06' or '11'
  int_ed <- as.integer(ed)
  prev_int_ed <- int_ed - 1
  char_prev_ed <- as.character(prev_int_ed)
  if(nchar(char_prev_ed) == 1) {
    char_prev_ed <- paste0("0", char_prev_ed)
  }
  return(char_prev_ed)
}

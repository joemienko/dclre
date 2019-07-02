theme_ts <- theme(panel.border = element_rect(fill = NA, 
                                              colour = "grey10"),
                  panel.background = element_blank(),
                  panel.grid.minor = element_line(colour = "grey85"),
                  panel.grid.major = element_line(colour = "grey85"),
                  panel.grid.major.x = element_line(colour = "grey85"),
                  axis.text = element_text(size = 13, face = "bold"),
                  axis.title = element_text(size = 15, face = "bold"),
                  plot.title = element_text(size = 16, face = "bold"),
                  strip.text = element_text(size = 16, face = "bold"),
                  strip.background = element_rect(colour = "black"),
                  legend.text = element_text(size = 15),
                  legend.title = element_text(size = 16, face = "bold"),
                  legend.background = element_rect(fill = "white"),
                  legend.key = element_rect(fill = "white"))


read_sql <- function(filename, silent = TRUE) {
  q <- readLines(filename, warn = !silent)
  q <- q[!grepl(pattern = "^\\s*--", x = q)] # remove full-line comments
  q <- sub(pattern = "--.*", replacement="", x = q) # remove midline comments
  q <- paste(q, collapse = " ")
  return(q)
}

split_data <- function(county) {
  dat_ts_all <- dat_time_series %>%
    filter(tx_county == county) %>%
    distinct()
  
  dat_ts_train <- dat_ts_all %>%
    filter(year < 2019)
  
  dat_ts_test <- dat_ts_all %>%
    filter(year == 2019)
  
  list(train = dat_ts_train
       ,test = dat_ts_test)
  
}
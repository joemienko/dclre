library(odbc)
library(RCurl)
library(purrr)
library(rpart) # decision tree method
library(party) # decision tree method
library(forecast) # forecasting methods
library(randomForest) # ensemble learning method
library(readr)
library(lubridate)
library(dplyr)
library(devtools)
#install_github("mikeasilva/blsAPI")
library(blsAPI)
library(tidycensus)
library(httr)
library(rvest)
library(twang)
library(ggplot2)
source('~/dclre/helper.R')
readRenviron(".env")

con <- odbc::dbConnect(odbc::odbc()
                       ,dsn = "POC"
                       ,uid = Sys.getenv("MSSQL_UN")
                       ,database = "CA_ODS"
                       ,pwd = Sys.getenv("MSSQL_PW")) 

court_ep_out <- DBI::dbGetQuery(con, read_sql("build_court_episodes.sql"))
join_out <- DBI::dbGetQuery(con, read_sql("build_day_county_join_table.sql"))
dat_propensity_scores <- DBI::dbGetQuery(con, read_sql("get_propensity_score_data.sql"))

ps_dclre <- ps(tx ~ .,
                 data = dat_propensity_scores %>%
                   select(-id_prsn_child
                          ,-id_removal_episode_fact) %>%
                   mutate(tx_gndr = as.factor(tx_gndr)
                          ,tx_braam_race = as.factor(tx_gndr)
                          ,tx_plcm_setng_first = as.factor(tx_plcm_setng_first)
                          ,tx_plcm_setng_longest = as.factor(tx_plcm_setng_longest)
                          ,tx_region = as.factor(tx_region)
                          ),
                 n.trees=5000,
                 interaction.depth=2,
                 shrinkage=0.01,
                 n.cores=4,
                 perm.test.iters=0,
                 stop.method=c("es.mean","ks.max"),
                 estimand = "ATT",
                 verbose=TRUE)

redcap_uri <- 'https://surveys.partnersforourchildren.org/api/'

requested_fields <- c('id_prsn_child',
                      'record_id',
                      'first_contact_complete',
                      'second_contact_complete',
                      'third_contact_complete',  
                      'youth_assent_form_under_12_complete',
                      'adult_respondent_consent_form_complete',
                      'guardian_consent_form_complete',
                      'guide_scheduler_complete',  
                      'child_participant_initialization_complete',
                      'child_legal_representation_survey_1_under_12_complete',
                      'child_legal_representation_survey_2_under_12_complete',
                      'child_legal_representation_survey_3_under_12_complete',
                      'child_no_representation_survey_1_under_12_complete',
                      'child_no_representation_survey_2_under_12_complete',
                      'child_no_representation_survey_3_under_12_complete',
                      'incentive_form_1_complete',
                      'incentive_form_2_complete',
                      'incentive_form_3_complete')

names(requested_fields) <- paste0('fields[', 0:18, ']')
       
post_body <- c(
  list(
  token                       = Sys.getenv("REDCAP_API_KEY_YES"),
  format                      = 'csv',
  type                        = 'flat',
  content                     = 'record',
  returnFormat                = 'csv',
  rawOrLabel                  = 'label',
  rawOrLabelHeaders           = 'label',
  exportCheckboxLabel         = 'true',
  exportSurveyFields          ='false',
  exportDataAccessGroups      ='false'
),
  as.list(requested_fields)
)

raw_text <- httr::POST(
  url                         = redcap_uri,
  body                        = post_body,
  config                      = httr::config(ssl_verifypeer=FALSE),
  httr::verbose() #Remove this line to suppress the frequent console updates.
)

dat_yes_progress <- httr::content(raw_text)

names(dat_yes_progress) <- requested_fields

census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

wa_fips_codes <- DBI::dbGetQuery(con, "select distinct fips from ##join_table") %>%
  .$fips

bls_unemp_codes <- paste0("LAUCN", wa_fips_codes, "0000000003")

decade1 <- list('seriesid'= bls_unemp_codes
                ,'startyear'='2005'
                ,'endyear'='2010') 

decade2 <- list('seriesid'= bls_unemp_codes
                              ,'startyear'='2011'
                              ,'endyear'='2019') 

bls_segmented_payload <- list(decade1, decade2)

month_county_unemployment <- map_df(.x = bls_segmented_payload
                       ,function(.x) {
                         blsAPI::blsAPI(payload = .x
                                        ,return_data_frame = T) %>%
                           mutate(fips = stringr::str_sub(seriesID, 6, 10)
                                  ,calendar_month = lubridate::as_date(paste0(year
                                                                              ,stringr::str_sub(period, 2)
                                                                              ,"01"))) 
                       })

month_county_removal_discharge <- DBI::dbGetQuery(con, read_sql("get_removal_and_discharge.sql")) %>%
  mutate(calendar_month = lubridate::as_date(paste0(lubridate::year(calendar_month)
                                                    ,stringr::str_pad(lubridate::month(calendar_month)
                                                                      ,2
                                                                      ,pad = "0")
                                                    ,"01"))
         ,fips = as.character(fips)
  )

month_county_removal_discharge_unemp <- left_join(month_county_removal_discharge
                                                   ,month_county_unemployment
                                                   ,by = c("calendar_month", "fips")
) %>%
  mutate(unemployment_rate = as.numeric(value)/100
         ,year = as.numeric(year)
         ,acs_end_year = case_when(
           year < 2007 ~ 2011,
           year %in% 2007:2010 ~ 2012,
           year >= 2011 & year <= 2013 ~ 2013,          
           year == 2014 ~ 2014,
           year == 2015 ~ 2015,
           year == 2016 ~ 2016,
           year > 2016 ~ 2017,
           TRUE ~ NA_real_
         )) %>%
  select(county_desc
         ,year
         ,acs_end_year
         ,month_name = periodName
         ,calendar_month
         ,fips
         ,unemployment_rate
         ,removal_count
         ,discharge_count)

census_tables_unique <- c("B01001_003"
                   ,"B01001_004"
                   ,"B01001_005"
                   ,"B01001_006"
                   ,"B01001_027"
                   ,"B01001_028"
                   ,"B01001_029"
                   ,"B01001_030")

fips_unique <- stringr::str_sub(month_county_removal_discharge_unemp$fips, 3) %>%
  unique()


survey_county_child_pop <- map_df(.x = 2011:2017
                       ,.y = fips_unique
                       ,function(.x, .y) {
  bind_cols(
  get_acs(geography = "county"
          ,variables = census_tables_unique
          ,survey  = "acs5"
          ,county = .y
          ,state = "WA"
          ,year = .x)
  ,data_frame(acs_end_year = rep(.x, 312))
  )
})

survey_county_child_pop_aggr <- survey_county_child_pop %>%
  group_by(GEOID, acs_end_year) %>%
  summarise(child_count = sum(estimate)
            ,moe_sum = sum(moe)
            ,moe_mean = mean(moe))

dat_time_series <- left_join(month_county_removal_discharge_unemp
           ,survey_county_child_pop_aggr
           ,by = c(fips = "GEOID", acs_end_year = "acs_end_year")) %>%
  mutate(removal_rate = removal_count*1000/child_count
         ,discharge_rate = discharge_count*1000/child_count
         ,month_number = month(calendar_month))

library(forecast)
library(tidyquant)
library(timetk)
library(sweep)

monthly_flow <- inner_join(
dat_time_series %>%
  mutate(month = lubridate::month(calendar_month, label = TRUE)) %>%
  group_by(month) %>%
  summarise(`Average Removal Rate` = mean(removal_rate, na.rm = TRUE)
            ,`Average Exit Rate` = mean(discharge_rate, na.rm = TRUE)) 
,dat_time_series %>%
  filter(year == 2018) %>%
  mutate(month = lubridate::month(calendar_month, label = TRUE)) %>%
  group_by(month) %>%
  summarise(`Average Removal Rate - 2018` = mean(removal_rate, na.rm = TRUE)
            ,`Average Exit Rate - 2018` = mean(discharge_rate, na.rm = TRUE)) 
) %>%
  gather("measure", "rate", -month) %>%
  mutate(current = as.factor(is.na(stringr::str_match(measure, "2018"))))


monthly_flow %>%
  ggplot(aes(x = month, y = rate, group = measure, colour = measure, linetype = current, size = current, alpha = current)) +
  geom_line(stat="smooth",method = "loess") +
  labs(title = "Dependency Dynamics by Month", x = "", y = "Dependency-Removals and Exits, Per 1000 Children",
       subtitle = "Average Rates, 2005 through April 2019") +
  scale_y_continuous() +
  scale_size_manual(values = c(1, 1.5)) + 
  scale_linetype_manual(values = c(1, 2)) + 
  scale_alpha_manual(values = c(1, .35)) + 
  scale_color_tq(theme = "dark") +
  theme_tq() + 
  guides(linetype=FALSE, size=FALSE, alpha=FALSE)

monthly_qty_by_county <- dat_time_series %>%
  filter(county_desc %in% c("Lewis", "Douglas", "Grant", "Whatcom")) %>%
  mutate(removal_month = as_date(as.yearmon(calendar_month))) %>%
  group_by(county_desc, removal_month) %>%
  summarise(total_removals = sum(removal_count))

monthly_qty_by_county_nest <- monthly_qty_by_county %>%
  group_by(county_desc) %>%
  nest(.key = "data.tbl")

monthly_qty_by_county_ts <- monthly_qty_by_county_nest %>%
  mutate(data.ts = map(.x       = data.tbl, 
                       .f       = tk_ts, 
                       select   = -removal_month, 
                       start    = 2005,
                       freq     = 12))

monthly_qty_by_county_fit <- monthly_qty_by_county_ts %>%
  mutate(fit.ets = map(data.ts, Arima, seasonal = c(0,1,1)))

monthly_qty_by_county_fit %>%
  mutate(tidy = map(fit.ets, sw_tidy)) %>%
  unnest(tidy, .drop = TRUE) %>%
  spread(key = county_desc, value = estimate)

monthly_qty_by_county_fit %>%
  mutate(glance = map(fit.ets, sw_glance)) %>%
  unnest(glance, .drop = TRUE)

augment_fit_ets <- monthly_qty_by_county_fit %>%
  mutate(augment = map(fit.ets, sw_augment, timetk_idx = TRUE, rename_index = "date")) %>%
  unnest(augment, .drop = TRUE)
                                             
augment_fit_ets %>%
  ggplot(aes(x = date, y = .resid, group = county_desc)) +
  geom_hline(yintercept = 0, color = "grey40") +
  geom_line(color = palette_light()[[2]]) +
  geom_smooth(method = "loess") +
  labs(title = "Removals By County",
       subtitle = "ETS Model Residuals", x = "") + 
  theme_tq() +
  facet_wrap(~ county_desc, scale = "free_y", ncol = 2) +
  scale_x_date(date_labels = "%Y")

monthly_qty_by_county_fit %>%
  mutate(decomp = map(fit.ets, sw_tidy, timetk_idx = TRUE, rename_index = "date")) %>%
  unnest(decomp)

monthly_qty_by_county_fcast <- monthly_qty_by_county_fit %>%
  mutate(fcast.ets = map(fit.ets, forecast, h = 12))

monthly_qty_by_county_fcast_tidy <- monthly_qty_by_county_fcast %>%
  mutate(sweep = map(fcast.ets, sw_sweep, fitted = FALSE, timetk_idx = TRUE)) %>%
  unnest(sweep)

send_to_zero <- function(x){
  ifelse(x < 0, 0, x)
}

monthly_qty_by_county_fcast_tidy %>%
  mutate_if(is.double, send_to_zero) %>%
  mutate(index = as.Date(index)) %>%
  ggplot(aes(x = index, y = total_removals, color = key, group = county_desc)) +
  geom_ribbon(aes(ymin = lo.95, ymax = hi.95), 
              fill = "#D5DBFF", color = NA, size = 0) +
  geom_ribbon(aes(ymin = lo.80, ymax = hi.80, fill = key), 
              fill = "#596DD5", color = NA, size = 0, alpha = 0.8) +
  geom_line() +
  labs(title = "Removals By County",
       subtitle = "Arima Model Forecasts",
       x = "", y = "Count of Dependency-Removals") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  scale_color_tq() +
  scale_fill_tq() +
  theme_tq() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  facet_wrap(~ county_desc, scales = "free_y", ncol = 2) 


monthly_dsc_by_county <- dat_time_series %>%
  filter(county_desc %in% c("Lewis", "Douglas", "Grant", "Whatcom")) %>%
  mutate(discharge_month = as_date(as.yearmon(calendar_month))) %>%
  group_by(county_desc, discharge_month) %>%
  summarise(total_discharge = sum(discharge_count))

monthly_dsc_by_county_nest <- monthly_dsc_by_county %>%
  group_by(county_desc) %>%
  nest(.key = "data.tbl")

monthly_dsc_by_county_ts <- monthly_dsc_by_county_nest %>%
  mutate(data.ts = map(.x       = data.tbl, 
                       .f       = tk_ts, 
                       select   = -discharge_month, 
                       start    = 2005,
                       freq     = 12))

monthly_dsc_by_county_fit <- monthly_dsc_by_county_ts %>%
  mutate(fit.ets = map(data.ts, Arima, seasonal = c(0,1,1)))

monthly_dsc_by_county_fit %>%
  mutate(tidy = map(fit.ets, sw_tidy)) %>%
  unnest(tidy, .drop = TRUE) %>%
  spread(key = county_desc, value = estimate)

monthly_dsc_by_county_fit %>%
  mutate(glance = map(fit.ets, sw_glance)) %>%
  unnest(glance, .drop = TRUE)

augment_fit_arima <- monthly_dsc_by_county_fit %>%
  mutate(augment = map(fit.ets, sw_augment, timetk_idx = TRUE, rename_index = "date")) %>%
  unnest(augment, .drop = TRUE)

augment_fit_arima %>%
  ggplot(aes(x = date, y = .resid, group = county_desc)) +
  geom_hline(yintercept = 0, color = "grey40") +
  geom_line(color = palette_light()[[2]]) +
  geom_smooth(method = "loess") +
  labs(title = "Discharge By County",
       subtitle = "Arima Model Residuals", x = "") + 
  theme_tq() +
  facet_wrap(~ county_desc, scale = "free_y", ncol = 2) +
  scale_x_date(date_labels = "%Y")

monthly_dsc_by_county_fit %>%
  mutate(decomp = map(fit.ets, sw_tidy, timetk_idx = TRUE, rename_index = "date")) %>%
  unnest(decomp)

monthly_dsc_by_county_fcast <- monthly_dsc_by_county_fit %>%
  mutate(fcast.ets = map(fit.ets, forecast, h = 12))

monthly_dsc_by_county_fcast_tidy <- monthly_dsc_by_county_fcast %>%
  mutate(sweep = map(fcast.ets, sw_sweep, fitted = FALSE, timetk_idx = TRUE)) %>%
  unnest(sweep)

monthly_dsc_by_county_fcast_tidy %>%
  mutate_if(is.double, send_to_zero) %>%
  mutate(index = as.Date(index)) %>%
  ggplot(aes(x = index, y = total_discharge, color = key, group = county_desc)) +
  geom_ribbon(aes(ymin = lo.95, ymax = hi.95), 
              fill = "#D5DBFF", color = NA, size = 0) +
  geom_ribbon(aes(ymin = lo.80, ymax = hi.80, fill = key), 
              fill = "#596DD5", color = NA, size = 0, alpha = 0.8) +
  geom_line() +
  labs(title = "Discharge By County",
       subtitle = "Arima Model Forecasts",
       x = "", y = "Count of Dependency-Removal Dismissals") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  scale_color_tq() +
  scale_fill_tq() +
  theme_tq() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  facet_wrap(~ county_desc, scales = "free_y", ncol = 2) 









dat_gts <- gts(as.matrix(dat_time_series_wide))

removal_forecast <- forecast(dat_gts)
forecasts <- aggts(removal_forecast, levels=0:1)
groups <- aggts(dat_gts, levels=0:1)
autoplot(forecasts) + autolayer(groups)

allf <- forecast(dat_gts, h=12,fmethod = 'ets')#here tdfp #means top-down forecast proportions and ets is roughly exponential #smoothing
plot(allf)


forecast(dat_gts, method="bu", fmethod="arima")


dat_raw <- split_data("Whatcom")

dat_ts <- ts(dat_raw$train$removal_rate
                     ,deltat = 1/12)

dat_decomp <- stl(dat_ts
                          ,s.window = "periodic"
                          ,robust = TRUE)$time.series

dat_trend_part <- ts(dat_decomp[,2])

trend_fit <- auto.arima(dat_trend_part) # ARIMA
trend_for <- as.vector(forecast(trend_fit, 365)$mean) # trend forecast

dat_msts <- msts(dat_raw$train$removal_rate
                  ,seasonal.periods = c(12, 365))

K <- 2
fuur <- fourier(dat_msts, K = c(K, K))

N <- nrow(dat_raw$train)
window <- (N / 12) - 1

new_load <- rowSums(dat_decomp[, c(1,3)]) # detrended original time series

lag_seas <- dat_decomp[1:(12*window), 1] # lag feature to model

matrix_train <- data.table(Load = tail(new_load, window*12),
                           fuur[(12 + 1):N,],
                           Lag = lag_seas)

# create testing data matrix
test_lag <- dat_decomp[((12*window)+1):N, 1]
fuur_test <- fourier(dat_msts, K = c(K, K), h = 12)

matrix_test <- data.table(fuur_test,
                          Lag = test_lag)

N_boot <- 100 # number of bootstraps

pred_mat <- matrix(0, nrow = N_boot, ncol = period)
for(i in 1:N_boot) {
  
  matrixSam <- matrix_train[sample(1:(N-period),
                                   floor((N-period) * sample(seq(0.7, 0.9, by = 0.01), 1)),
                                   replace = TRUE)] # sampling with sampled ratio from 0.7 to 0.9
  tree_bag <- rpart(Load ~ ., data = matrixSam,
                    control = rpart.control(minsplit = sample(2:3, 1),
                                            maxdepth = sample(26:30, 1),
                                            cp = sample(seq(0.0000009, 0.00001, by = 0.0000001), 1)))
  
  # new data and prediction
  pred_mat[i,] <- predict(tree_bag, matrix_test) + mean(trend_for)
}



rf_model <- randomForest(Load ~ ., data = matrix_train,
                         ntree = 1000, mtry = 3, nodesize = 5, importance = TRUE)





dat_ts_whatcom_train <- dat_ts_whatcom %>%
  filter(year < 2019)

dat_ts_whatcom_test <- dat_ts_whatcom %>%
  filter(year == 2019)

ggplot(dat_ts_whatcom_train, aes(calendar_month, removal_rate)) +
  geom_line() +
  labs(x = "Date", y = "Load (kW)") +
  theme_ts










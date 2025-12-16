#' @importFrom magrittr %>%

# Für R CMD check: sichtbare Bindungen für NSE-Variablen und Pipes
utils::globalVariables(c(
  "parametertype_name", "station_name", "river_name", "catchment_name", "ts_name", "."
))


#' Safe URL Fetch with Curl
#'
#' This function attempts to fetch a URL with SSL certificate verification.
#' If verification fails, the URL is fetched again without certificate checking.
#' Optionally, SSL errors can be logged.
#'
#' @param full_url Character. The full URL to fetch.
#' @param logPath Character or NULL. Path to a directory where SSL errors will be logged. Default is NULL (no logging).
#'
#' @return A curl connection object representing the requested URL.
#'
#' @details
#' The function first tries to fetch the URL with SSL certificate verification enabled.
#' If this fails, the error is optionally logged to a file named `ssl.error` in the specified log path,
#' and the URL is fetched again without SSL verification.
#'
#' @examples
#' \dontrun{
#' # Fetch a secure URL
#' con <- safe_curl("https://example.com", logPath = tempdir())
#' readLines(con)
#' close(con)
#' }
#'
#' @importFrom curl new_handle handle_setopt curl
#' @importFrom readr write_lines
#' @export

safe_curl <- function(full_url, logPath = NULL) {

  h <- new_handle()
  handle_setopt(h, ssl_verifypeer = 1, ssl_verifyhost = 2)
  
  con <- tryCatch(
    {
      curl(full_url, handle = h)
    },
    error = function(e) {
      # Wenn Fehler: ins Log schreiben
      if (!is.null(logPath)) {
        log_file <- file.path(logPath, "ssl.error")
        readr::write_lines(
          paste(Sys.time(), "- SSL-Fehler bei", full_url, ":", conditionMessage(e)),
          log_file,
          append = TRUE
        )
      }
    
      h2 <- new_handle()
      handle_setopt(h2, ssl_verifypeer = 0, ssl_verifyhost = 0)
      curl(full_url, handle = h2)
    }
  )
  
  return(con)
}

#' Check Internet Connectivity
#'
#' This function checks if an internet connection is available by attempting 
#' to read a line from a known URL.
#'
#' @return Logical. `TRUE` if the internet connection is available, `FALSE` otherwise.
#'
#' @details
#' The function attempts to fetch the first line from "https://www.google.de".
#' Any warnings or errors during this process are suppressed. 
#' If the attempt fails, the function returns `FALSE`.
#'
#' @examples
#' if (has_internet()) {
#'   message("Internet connection available")
#' } else {
#'   message("No internet connection")
#' }
#'
#' @export
has_internet <- function(){
  z <- try(suppressWarnings(
    readLines('https://www.google.de', n = 1)
    ), silent = TRUE)
  !inherits(z, "try-error")
}

#' Test if an Experiment Hub is Live via Curl
#'
#' This function checks if an Experiment Hub is reachable by querying a specific URL.
#' It first attempts to connect with SSL verification enabled. If that fails, it retries
#' without SSL verification. Optional logging records either the IP or any errors encountered.
#'
#' @param hub Character. Base URL of the Experiment Hub.
#' @param logPath Character or NULL. Path to a directory where logs will be written. Default is NULL (no logging).
#'
#' @return Logical. `TRUE` if the hub is reachable, `FALSE` otherwise.
#'
#' @details
#' The function constructs a URL to request the station list in JSON format.
#' It tries to retrieve the hub's IP address using `curl` first with SSL verification.
#' If the SSL-secured attempt fails, it retries without SSL verification and logs any errors.
#' If `logPath` is provided, the hub's IP or any errors are appended to `hub_ip.log`.
#'
#' @examples
#' \dontrun{
#' exp_live_curl("https://my.experimenthub.org", logPath = tempdir())
#' }
#'
#' @importFrom readr write_lines
#' @export
exp_live_curl <- function(hub, logPath = NULL) {
  
  exp_hub_url <- paste0(
    hub,
    "datasource=0&service=kisters&type=queryServices&request=getstationlist&format=json"
  )
  
  null_device <- ifelse(.Platform$OS.type == "windows", "NUL", "/dev/null")
  
  try_ssl <- function(ssl_verifypeer, ssl_verifyhost) {
    curl_cmd <- c(
      if (ssl_verifypeer == 0) "-k" else "-sS",
      "-sS",
      "--max-time", "15",
      "-o", null_device,
      "-w", "%{remote_ip}",
      exp_hub_url
    )
    system2("curl", args = curl_cmd, stdout = TRUE, stderr = TRUE)
  }
  
  res <- tryCatch({
    ip <- try_ssl(ssl_verifypeer = 1, ssl_verifyhost = 2)
    if (inherits(ip, "try-error")) {
      if (!is.null(logPath)) {
        log_file <- file.path(logPath, "hub_ip.log")
        readr::write_lines(
          paste(Sys.time(), "-", hub, "- SSL error, retrying without SSL"),
          log_file,
          append = TRUE
        )
      }
      ip <- try_ssl(ssl_verifypeer = 0, ssl_verifyhost = 0)
    }
    
    if (!is.null(logPath)) {
      log_file <- file.path(logPath, "hub_ip.log")
      readr::write_lines(
        paste(Sys.time(), "-", hub, "-", ip),
        log_file,
        append = TRUE
      )
    }
    
    list(success = TRUE, ip = ip)
    
  }, error = function(e) {
    if (!is.null(logPath)) {
      log_file <- file.path(logPath, "hub_ip.log")
      readr::write_lines(
        paste(Sys.time(), "-", hub, "- Error:", conditionMessage(e)),
        log_file,
        append = TRUE
      )
    }
    list(success = FALSE, ip = NA)
  })
  
  return(res$success)
}


#' Retrieve KIWIS Station Metadata from an Experiment Hub
#'
#' This function fetches metadata for stations from a KIWIS Experiment Hub.
#' Users can specify which fields to return, filter by data type, and exclude certain stations by pattern.
#'
#' @param hub Character. Base URL of the Experiment Hub.
#' @param dataType Character vector or NULL. Filter for specific parameter types (e.g., "WaterLevel"). Default is NULL (no filtering).
#' @param return_fields Character vector or NULL. Fields to include in the output. Default includes station name, number, river, catchment, latitude, longitude, and parameter type.
#' @param exclude_patterns Character vector or NULL. Patterns to exclude stations by `station_name`. Default is NULL.
#' @param datasource Integer. Data source ID for the hub query. Default is 1.
#' @param logPath Character or NULL. Path to a directory for logging SSL errors during URL retrieval. Default is NULL (no logging).
#'
#' @return A data.frame containing metadata for KIWIS stations.  
#'   Includes a `heading` column with HTML-formatted station information.
#'
#' @details
#' The function constructs a URL query to the Experiment Hub using the specified fields and filters.
#' Data is retrieved using `safe_curl()` to handle SSL issues. The first row of the returned JSON
#' is used as column names. Filtering by `dataType` and exclusion patterns is applied after data retrieval.
#' The `heading` column is generated for convenient HTML display.
#'
#' @examples
#' \dontrun{
#' # Fetch all stations from the hub
#' meta <- get_kiwis_meta("http://kiwis.kisters.de/KiWIS/KiWIS", datasource = 0)
#'
#' # Fetch only water level stations, excluding certain station names
#' meta_filtered <- get_kiwis_meta(
#'   "http://kiwis.kisters.de/KiWIS/KiWIS",
#'   datasource=0,
#'   dataType = "Precip",
#'   exclude_patterns = c("Zwiesel", "Test")
#' )
#' }
#'
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_detect regex
#' @importFrom dplyr filter mutate
#' @importFrom utils URLencode
#' @export

get_kiwis_meta <- function(hub, dataType = NULL, return_fields = NULL, exclude_patterns = NULL, datasource = 1, logPath = NULL) {

  if (is.null(return_fields) || length(return_fields) == 0) {
    return_fields <- c(
      "station_name",
      "station_no",
      "river_name",
      "catchment_name",
      "station_latitude",
      "station_longitude",
      "parametertype_name"
    )
  }

  query <- list(
    service = "kisters",
    datasource = as.character(datasource),
    type = "queryServices",
    request = "getStationList",
    format = "json",
    kvp = "true",
    returnfields = paste(return_fields, collapse = ",")
  )

  query_string <- paste0(
    names(query), "=", vapply(query, URLencode, character(1), reserved = TRUE),
    collapse = "&"
  )

  full_url <- paste0(hub, "?", query_string)

  con <- safe_curl(full_url, logPath)
  raw_data <-  fromJSON(con)

  col_names <- unlist(raw_data[1, ])
  raw_data <- raw_data[-1, , drop = FALSE]

  data_vals<-as.data.frame(raw_data,.name_repair= ~make.unique(.))

  colnames(data_vals) <- as.character(col_names)

  if (!is.null(dataType)) {
    data_vals <- data_vals  %>%
      filter(parametertype_name %in% dataType)
  }

  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    pattern_regex <- paste0(exclude_patterns, collapse = "|")
    data_vals <- data_vals[!str_detect(data_vals$station_name, regex(pattern_regex, ignore_case = TRUE)), ]
  }

    data_vals <- data_vals %>%
    mutate(heading = paste(
      paste0("Pegel: ", station_name),
      paste0("Gew: ", river_name),
      paste0("EZG: ", catchment_name),
      sep = "<br>"
    ))


  return(data_vals)
}

#' Retrieve a List of KIWIS Time Series
#'
#' This function queries a KIWIS hub and retrieves a list of time series metadata for specified stations and data types. 
#' It allows filtering by station number, time series names, and parameter types, and supports returning specific fields.
#'
#' @param hub Character. URL of the KIWIS hub to query.
#' @param station_no Character or numeric vector. Station numbers to filter by. If NULL or empty, all stations are returned.
#' @param dataType Character vector. Parameter types to filter the time series. Defaults to all types ("*").
#' @param return_fields Character vector. Specific fields to return from the time series metadata. Defaults to commonly used fields.
#' @param include_ts_name Character vector. Time series names to include. Defaults to all ("*").
#' @param exclude_ts_names Character vector. Time series names to exclude from the results.
#' @param datasource Numeric or character. Identifier for the data source. Default is 1.
#' @param logPath Character. Optional path to log queries and responses.
#'
#' @return A data frame containing the requested time series metadata. Column names correspond to the fields specified in `return_fields`.
#'
#' @examples
#' \dontrun{
#' hub_url <- "http://kiwis.kisters.de/KiWIS/KiWIS"
#' ts_list <- get_kiwis_ts_list(hub = hub_url, dataType = "Precip", datasource = 0)
#' head(ts_list)
#' }
#'
#' @export


get_kiwis_ts_list <- function(hub, station_no=NULL, dataType=NULL, return_fields=NULL, include_ts_name=NULL, exclude_ts_names=NULL, datasource = 1, logPath = NULL) {
  
  if (!is.null(station_no) || length(station_no) == 0) {
    station_no <- "*" # to avoid filtering by station_no
      
  } else if (length(station_no)> 1) {
     station_no <- paste0(station_no, collapse = ",")
  }

  if (is.null(return_fields) || length(return_fields) == 0) {
    return_fields <- c(
      "station_name",
      "site_name",
      "station_no",
      "ts_id",
      "ts_name",
      "parametertype_name",
      "coverage",
      "ts_clientvalue1",
      "ts_clientvalue6",
      "ts_clientvalue7"
    )
  }
  
  if (is.null(dataType)) {
    dataType =  "*" # to avoid filtering by dataType  
  }
  
if(!is.null(include_ts_name) || length(include_ts_name) != 0) {
    include_ts_name <- paste0(include_ts_name, collapse = ",")
  } else if (is.null(include_ts_name)) {
    include_ts_name <- "*"
      } 


  query <- list(
    service = "kisters",
    datasource = as.character(datasource),
    type = "queryServices",
    request = "getTimeseriesList",
    format = "tabjson",
    kvp = "true",
    station_no = station_no,
    ts_name = include_ts_name,
    parametertype_name = dataType,
    returnfields = paste(return_fields, collapse = ",")
  )

  query_string <- paste0(
    names(query), "=", vapply(query, URLencode, character(1), reserved = TRUE),
    collapse = "&"
  )

  full_url <- paste0(hub, "?", query_string)

  con <- safe_curl(full_url, logPath)
  raw_data <-  as.data.frame(fromJSON(con))
  colnames(raw_data) <- raw_data[1, ]
  raw_data <- raw_data[-1, , drop = FALSE]

  if (!is.null( exclude_ts_names) && length(exclude_ts_names) > 0) {
    raw_data <- raw_data %>% filter(!str_detect(ts_name, paste0(exclude_ts_names, collapse = "|")))
  } 
  
  return(raw_data)
}

#' Retrieve KIWIS Time Series Data
#'
#' This function queries a KIWIS hub to retrieve time series data for specified time series IDs (`data_id`) over a given date range.
#' For large sets of IDs, the function automatically splits the request into blocks to avoid exceeding service limits.
#' It also allows selecting specific fields for both the time series values and metadata.
#'
#' @param hub Character. URL of the KIWIS hub to query.
#' @param data_id Character or numeric vector. IDs of the time series to retrieve.
#' @param dateRange Date vector of length 2. Start and end dates for the time series data.
#' @param format Character. Format of the returned data. Options are "zrxp" (default), "tabjson", or "dajson".
#' @param return_fields Character vector. Fields to return for each data point. Defaults to c("Timestamp", "Value").
#' @param md_returnfields Character vector. Metadata fields to return. Defaults to c("station_no", "ts_clientvalue6", "ts_clientvalue7").
#' @param truncation_fac Numeric. Maximum number of IDs per request block. Default is 50.
#' @param datasource Numeric or character. Identifier for the data source. Default is 1.
#' @param logPath Character. Optional path to log queries and responses.
#'
#' @return A character vector containing the raw response lines from the KIWIS hub for all requested time series. Each block is concatenated sequentially.
#'
#' @examples
#' \dontrun{
#' hub_url <- "http://kiwis.kisters.de/KiWIS/KiWIS"
#' ts_data <- get_kiwis_ts(hub = hub_url, datasource = 0,
#'                         data_id = c("3388042", "3448042"), 
#'                         dateRange = as.Date(c("2020-01-01", "2021-01-31")))
#' head(ts_data)
#' }
#'
#' @export
get_kiwis_ts <- function(hub, data_id, dateRange, format=NULL, return_fields=NULL, md_returnfields=NULL, truncation_fac=50,datasource=1, logPath = NULL)
{
    if(is.null(format)) 
    {
      format="dajson"
    }

    if (length(data_id) >= truncation_fac) {
      trunc <- unique(c(seq(1, length(data_id), truncation_fac), length(data_id)))
  
    ids <- vapply(seq_along(trunc)[-1], function(i) {
      idx_range <- seq(trunc[i-1], trunc[i])   # robust statt ":"
      paste0(unique(data_id[idx_range]), collapse = ",")
    }, character(1))
  
  } else {
  # no truncation needed
  ids <- paste0(unique(data_id), collapse = ",")
  trunc <- c(1, length(data_id))
  }


    if (is.null(return_fields) || length(return_fields) == 0) {
    return_fields <- c(
      "Timestamp",
      "Value"
    )
  }

    if (is.null(md_returnfields) || length(md_returnfields) == 0) {
    md_returnfields <- c(
      "ts_id",
      "station_no",
      "ts_clientvalue6",
      "ts_clientvalue7"
    )
  }

  for (i in 2:length(trunc))
    {
  
    
    query <- list(
    service = "kisters",
    datasource = as.character(datasource),
    type = "queryServices",
    request = "getTimeseriesValues",
    format = format,
    ts_id = as.character(ids[i-1]),
    from = as.character(dateRange[1]),
    to = as.character(dateRange[2]),
    metadata="true",
    returnfields = paste(return_fields, collapse = ","),
    md_returnfields= paste(md_returnfields, collapse = ",")
    )

  query_string <- paste0(
    names(query), "=", vapply(query, URLencode, character(1), reserved = TRUE),
    collapse = "&"
  )

  full_url <- paste0(hub, "?", query_string)
  
  infoCon <- safe_curl(full_url, logPath)
  
  if(i==2) output <- readLines(infoCon)
  if(i>2) output<-append(output,readLines(infoCon))
  
  close(infoCon)
  }
  
  return(output)
}

#' Write Dataset in ZRXP Format for AquaZIS
#'
#' This function converts a dataset to be compatible with the ZRXP format required by AquaZIS.
#' It renames specific columns (`ts_clientvalue6`, `EXTNUM1`, `ts_clientvalue7`,`EXTNUM2`), 
#' ensures UTF-8 encoding, removes any existing ZRXP headers, and writes the result to a file.
#'
#' @param path Character. File path where the converted dataset will be saved.
#' @param dataset Character vector. The dataset to be converted and written to file. Typically obtained from KIWIS time series queries.
#'
#' @return None. The function writes the processed dataset to the specified file path.
#'
#' @examples
#' \dontrun{
#' data_lines <- c("ts_clientvalue6,ts_clientvalue7,Value", "123,456,7.89")
#' write_zrxp_for_aquazis(path = "output.zrxp", dataset = data_lines)
#' }
#'
#' @export

write_zrxp_for_aquazis <- function(path, dataset)
{
  
  #change column names for zrxp compatibility
  extNumPos <- grep("ts_clientvalue6*", dataset, useBytes=TRUE)
  dataset[extNumPos] <- iconv(dataset[extNumPos], from = "", to = "UTF-8", sub = "")
  dataset[extNumPos] <- gsub("ts_clientvalue6", "EXTNUM1", dataset[extNumPos])

  extNumPos <- grep("ts_clientvalue7*", dataset, useBytes=TRUE)
  dataset[extNumPos] <- iconv(dataset[extNumPos], from = "", to = "UTF-8", sub = "")
  dataset[extNumPos] <- gsub("ts_clientvalue7", "EXTNUM2", dataset[extNumPos])

  extNumPos <- grep("ZRXP*", dataset, useBytes=TRUE)
  dataset[extNumPos] <- iconv(dataset[extNumPos], from = "", to = "UTF-8", sub = "")
  dataset <- dataset[-extNumPos]
 #print(utils::head(kiwis_ts, 10))  # sicherer als [1:10], falls < 10 Zeilen
  
con <- file(path, open = "w", encoding = "UTF-8")
writeLines(dataset,con,useBytes=TRUE)
flush(con)
close(con)

}


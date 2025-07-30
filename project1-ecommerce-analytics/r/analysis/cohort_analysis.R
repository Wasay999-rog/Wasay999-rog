# ===================================================================
# E-commerce Customer Cohort Analysis in R
# Advanced retention analysis and customer lifetime value modeling
# ===================================================================

# Load required libraries
suppressMessages({
  library(dplyr)
  library(ggplot2)
  library(plotly)
  library(DT)
  library(scales)
  library(lubridate)
  library(RColorBrewer)
  library(reshape2)
  library(corrplot)
  library(survival)
  library(survminer)
  library(forecast)
  library(DBI)
  library(RSnowflake)
  library(openxlsx)
})

# ===================================================================
# Configuration and Database Connection
# ===================================================================

# Database connection parameters
source("../config/snowflake_config.R")

# Connect to Snowflake
connect_to_snowflake <- function() {
  conn <- dbConnect(
    RSnowflake::snowflake(),
    account = SNOWFLAKE_ACCOUNT,
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = "ECOMMERCE_DW",
    schema = "ANALYTICS"
  )
  return(conn)
}

# ===================================================================
# Data Extraction Functions
# ===================================================================

extract_customer_data <- function(conn) {
  query <- "
  SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.registration_date,
    c.age,
    c.gender,
    c.country,
    o.order_id,
    o.order_date,
    o.total_amount,
    DATE_TRUNC('month', o.order_date) as order_month,
    DATE_TRUNC('month', c.registration_date) as cohort_month,
    DATEDIFF('month', 
             DATE_TRUNC('month', c.registration_date), 
             DATE_TRUNC('month', o.order_date)) as period_number
  FROM ECOMMERCE_DW.STAGING.stg_customers c
  LEFT JOIN ECOMMERCE_DW.RAW_DATA.orders o ON c.customer_id = o.customer_id
  WHERE o.order_status = 'completed'
  ORDER BY c.customer_id, o.order_date
  "
  
  data <- dbGetQuery(conn, query)
  data$order_date <- as.Date(data$order_date)
  data$registration_date <- as.Date(data$registration_date)
  data$cohort_month <- as.Date(data$cohort_month)
  data$order_month <- as.Date(data$order_month)
  
  return(data)
}

# ===================================================================
# Cohort Analysis Functions
# ===================================================================

calculate_cohort_table <- function(data) {
  # Create cohort table with customer counts
  cohort_data <- data %>%
    group_by(cohort_month, period_number) %>%
    summarise(
      customers = n_distinct(customer_id),
      revenue = sum(total_amount, na.rm = TRUE),
      orders = n_distinct(order_id),
      avg_order_value = mean(total_amount, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Calculate cohort sizes (period 0)
  cohort_sizes <- cohort_data %>%
    filter(period_number == 0) %>%
    select(cohort_month, cohort_size = customers)
  
  # Calculate retention rates
  cohort_table <- cohort_data %>%
    left_join(cohort_sizes, by = "cohort_month") %>%
    mutate(
      retention_rate = customers / cohort_size,
      revenue_per_customer = revenue / customers,
      orders_per_customer = orders / customers
    ) %>%
    arrange(cohort_month, period_number)
  
  return(cohort_table)
}

create_retention_heatmap <- function(cohort_table) {
  # Prepare data for heatmap
  retention_matrix <- cohort_table %>%
    select(cohort_month, period_number, retention_rate) %>%
    pivot_wider(names_from = period_number, values_from = retention_rate, names_prefix = "Period_")
  
  # Convert to matrix for heatmap
  matrix_data <- as.matrix(retention_matrix[, -1])
  rownames(matrix_data) <- format(retention_matrix$cohort_month, "%Y-%m")
  
  # Create heatmap
  heatmap_plot <- ggplot(
    melt(matrix_data, na.rm = TRUE), 
    aes(Var2, Var1, fill = value)
  ) +
    geom_tile(color = "white") +
    scale_fill_gradient2(
      low = "red", 
      mid = "yellow", 
      high = "green",
      midpoint = 0.5,
      labels = percent_format(),
      name = "Retention\nRate"
    ) +
    labs(
      title = "Customer Retention Cohort Analysis",
      subtitle = "Retention rates by cohort month and period",
      x = "Period Number",
      y = "Cohort Month"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      plot.title = element_text(size = 16, face = "bold"),
      plot.subtitle = element_text(size = 12)
    )
  
  return(heatmap_plot)
}

# ===================================================================
# Customer Lifetime Value Analysis
# ===================================================================

calculate_clv_metrics <- function(data) {
  # Calculate customer-level metrics
  customer_metrics <- data %>%
    group_by(customer_id) %>%
    summarise(
      first_purchase = min(order_date, na.rm = TRUE),
      last_purchase = max(order_date, na.rm = TRUE),
      total_orders = n_distinct(order_id),
      total_revenue = sum(total_amount, na.rm = TRUE),
      avg_order_value = mean(total_amount, na.rm = TRUE),
      customer_lifespan = as.numeric(last_purchase - first_purchase),
      avg_days_between_orders = ifelse(total_orders > 1, 
                                      customer_lifespan / (total_orders - 1), 
                                      NA),
      .groups = 'drop'
    ) %>%
    mutate(
      purchase_frequency = total_orders / pmax(customer_lifespan / 365, 1/365),
      predicted_clv = total_revenue * (purchase_frequency / 365) * 365 * 2  # 2-year projection
    )
  
  return(customer_metrics)
}

create_clv_segmentation <- function(customer_metrics) {
  # Create CLV segments using quantiles
  clv_segments <- customer_metrics %>%
    mutate(
      clv_segment = case_when(
        predicted_clv >= quantile(predicted_clv, 0.8, na.rm = TRUE) ~ "High Value",
        predicted_clv >= quantile(predicted_clv, 0.6, na.rm = TRUE) ~ "Medium-High Value",
        predicted_clv >= quantile(predicted_clv, 0.4, na.rm = TRUE) ~ "Medium Value",
        predicted_clv >= quantile(predicted_clv, 0.2, na.rm = TRUE) ~ "Medium-Low Value",
        TRUE ~ "Low Value"
      ),
      frequency_segment = case_when(
        purchase_frequency >= quantile(purchase_frequency, 0.8, na.rm = TRUE) ~ "Very Frequent",
        purchase_frequency >= quantile(purchase_frequency, 0.6, na.rm = TRUE) ~ "Frequent",
        purchase_frequency >= quantile(purchase_frequency, 0.4, na.rm = TRUE) ~ "Moderate",
        purchase_frequency >= quantile(purchase_frequency, 0.2, na.rm = TRUE) ~ "Occasional",
        TRUE ~ "Rare"
      )
    )
  
  return(clv_segments)
}

# ===================================================================
# Survival Analysis for Customer Churn
# ===================================================================

prepare_survival_data <- function(data) {
  # Prepare data for survival analysis
  customer_survival <- data %>%
    group_by(customer_id) %>%
    summarise(
      first_purchase = min(order_date, na.rm = TRUE),
      last_purchase = max(order_date, na.rm = TRUE),
      total_orders = n_distinct(order_id),
      total_revenue = sum(total_amount, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    mutate(
      time_to_last_purchase = as.numeric(last_purchase - first_purchase),
      # Consider customer churned if no purchase in last 90 days
      churned = as.numeric(Sys.Date() - last_purchase > 90),
      # Time from first purchase to either churn or censoring
      survival_time = ifelse(churned == 1, 
                           time_to_last_purchase, 
                           as.numeric(Sys.Date() - first_purchase))
    ) %>%
    filter(survival_time > 0)
  
  return(customer_survival)
}

perform_survival_analysis <- function(survival_data) {
  # Fit Kaplan-Meier survival curve
  km_fit <- survfit(Surv(survival_time, churned) ~ 1, data = survival_data)
  
  # Create survival plot
  survival_plot <- ggsurvplot(
    km_fit,
    data = survival_data,
    conf.int = TRUE,
    pval = TRUE,
    risk.table = TRUE,
    title = "Customer Survival Analysis",
    subtitle = "Time to Customer Churn",
    xlab = "Days since First Purchase",
    ylab = "Survival Probability (Retention)",
    legend.title = "Overall",
    legend.labs = "All Customers"
  )
  
  return(list(fit = km_fit, plot = survival_plot))
}

# ===================================================================
# Revenue Forecasting
# ===================================================================

forecast_revenue <- function(cohort_table) {
  # Aggregate monthly revenue
  monthly_revenue <- cohort_table %>%
    group_by(order_month = cohort_month + months(period_number)) %>%
    summarise(total_revenue = sum(revenue, na.rm = TRUE), .groups = 'drop') %>%
    filter(!is.na(order_month)) %>%
    arrange(order_month)
  
  # Create time series
  revenue_ts <- ts(monthly_revenue$total_revenue, 
                  start = c(year(min(monthly_revenue$order_month)), 
                           month(min(monthly_revenue$order_month))),
                  frequency = 12)
  
  # Fit ARIMA model
  arima_model <- auto.arima(revenue_ts)
  
  # Generate forecast
  forecast_result <- forecast(arima_model, h = 12)
  
  # Create forecast plot
  forecast_plot <- autoplot(forecast_result) +
    labs(
      title = "Revenue Forecast - Next 12 Months",
      subtitle = paste("ARIMA Model:", arima_model$call$order),
      x = "Time",
      y = "Revenue"
    ) +
    theme_minimal()
  
  return(list(model = arima_model, forecast = forecast_result, plot = forecast_plot))
}

# ===================================================================
# Visualization Functions
# ===================================================================

create_cohort_summary_dashboard <- function(cohort_table, customer_metrics) {
  # 1. Retention by Cohort Size
  p1 <- cohort_table %>%
    filter(period_number <= 12) %>%
    ggplot(aes(x = period_number, y = retention_rate, color = factor(cohort_month))) +
    geom_line(size = 1) +
    scale_y_continuous(labels = percent_format()) +
    labs(
      title = "Retention Rates by Cohort",
      x = "Period (Months)",
      y = "Retention Rate",
      color = "Cohort Month"
    ) +
    theme_minimal() +
    theme(legend.position = "none")  # Too many cohorts for legend
  
  # 2. Average Revenue per Customer by Period
  p2 <- cohort_table %>%
    filter(period_number <= 12) %>%
    ggplot(aes(x = period_number, y = revenue_per_customer)) +
    geom_boxplot(aes(group = period_number), alpha = 0.6) +
    geom_smooth(method = "loess", se = TRUE, color = "red") +
    labs(
      title = "Revenue per Customer by Period",
      x = "Period (Months)",
      y = "Revenue per Customer"
    ) +
    theme_minimal()
  
  # 3. CLV Distribution
  p3 <- customer_metrics %>%
    ggplot(aes(x = predicted_clv)) +
    geom_histogram(bins = 50, fill = "skyblue", alpha = 0.7) +
    scale_x_continuous(labels = dollar_format()) +
    labs(
      title = "Customer Lifetime Value Distribution",
      x = "Predicted CLV",
      y = "Number of Customers"
    ) +
    theme_minimal()
  
  # 4. Purchase Frequency vs CLV
  p4 <- customer_metrics %>%
    ggplot(aes(x = purchase_frequency, y = predicted_clv)) +
    geom_point(alpha = 0.6, color = "darkblue") +
    geom_smooth(method = "lm", se = TRUE, color = "red") +
    scale_y_continuous(labels = dollar_format()) +
    labs(
      title = "Purchase Frequency vs CLV",
      x = "Purchase Frequency (orders per year)",
      y = "Predicted CLV"
    ) +
    theme_minimal()
  
  return(list(p1 = p1, p2 = p2, p3 = p3, p4 = p4))
}

# ===================================================================
# Report Generation Functions
# ===================================================================

generate_cohort_report <- function(cohort_table, customer_metrics, survival_analysis) {
  # Calculate summary statistics
  avg_retention_month_1 <- mean(cohort_table$retention_rate[cohort_table$period_number == 1], na.rm = TRUE)
  avg_retention_month_6 <- mean(cohort_table$retention_rate[cohort_table$period_number == 6], na.rm = TRUE)
  avg_retention_month_12 <- mean(cohort_table$retention_rate[cohort_table$period_number == 12], na.rm = TRUE)
  
  avg_clv <- mean(customer_metrics$predicted_clv, na.rm = TRUE)
  median_clv <- median(customer_metrics$predicted_clv, na.rm = TRUE)
  
  total_customers <- n_distinct(customer_metrics$customer_id)
  total_revenue <- sum(customer_metrics$total_revenue, na.rm = TRUE)
  
  # Create summary report
  report <- list(
    summary_stats = data.frame(
      Metric = c("Total Customers", "Total Revenue", "Average CLV", "Median CLV",
                "1-Month Retention", "6-Month Retention", "12-Month Retention"),
      Value = c(
        format(total_customers, big.mark = ","),
        paste0("$", format(total_revenue, big.mark = ",", digits = 0)),
        paste0("$", format(avg_clv, big.mark = ",", digits = 0)),
        paste0("$", format(median_clv, big.mark = ",", digits = 0)),
        paste0(round(avg_retention_month_1 * 100, 1), "%"),
        paste0(round(avg_retention_month_6 * 100, 1), "%"),
        paste0(round(avg_retention_month_12 * 100, 1), "%")
      )
    ),
    
    key_insights = c(
      paste0("Average 1-month retention rate is ", round(avg_retention_month_1 * 100, 1), "%"),
      paste0("Average CLV is $", format(avg_clv, big.mark = ",", digits = 0)),
      paste0("Top 20% of customers represent ", 
             round(sum(customer_metrics$total_revenue[customer_metrics$predicted_clv >= quantile(customer_metrics$predicted_clv, 0.8, na.rm = TRUE)], na.rm = TRUE) / total_revenue * 100, 1), 
             "% of total revenue"),
      "Retention rates decline significantly after the first month",
      "High-frequency customers have significantly higher CLV"
    ),
    
    recommendations = c(
      "Implement aggressive retention programs for new customers in their first month",
      "Develop loyalty programs targeting medium-value customers for CLV growth",
      "Create specialized retention campaigns for at-risk high-value customers",
      "Investigate factors driving customer churn in the first 30 days",
      "Consider personalized marketing based on purchase frequency segments"
    )
  )
  
  return(report)
}

export_results_to_excel <- function(cohort_table, customer_metrics, clv_segments, report, filename = "cohort_analysis_results.xlsx") {
  # Create workbook
  wb <- createWorkbook()
  
  # Add worksheets
  addWorksheet(wb, "Executive Summary")
  addWorksheet(wb, "Cohort Table")
  addWorksheet(wb, "Customer Metrics")
  addWorksheet(wb, "CLV Segments")
  addWorksheet(wb, "Retention Heatmap Data")
  
  # Write data to worksheets
  writeData(wb, "Executive Summary", report$summary_stats, startRow = 1)
  writeData(wb, "Executive Summary", data.frame(Insights = report$key_insights), startRow = 10)
  writeData(wb, "Executive Summary", data.frame(Recommendations = report$recommendations), startRow = 20)
  
  writeData(wb, "Cohort Table", cohort_table)
  writeData(wb, "Customer Metrics", customer_metrics)
  writeData(wb, "CLV Segments", clv_segments)
  
  # Add formatting
  addStyle(wb, "Executive Summary", createStyle(textDecoration = "bold"), rows = 1, cols = 1:2)
  addStyle(wb, "Executive Summary", createStyle(textDecoration = "bold"), rows = 10, cols = 1)
  addStyle(wb, "Executive Summary", createStyle(textDecoration = "bold"), rows = 20, cols = 1)
  
  # Save workbook
  saveWorkbook(wb, filename, overwrite = TRUE)
  cat(paste("Results exported to", filename, "\n"))
}

# ===================================================================
# Main Analysis Workflow
# ===================================================================

main_cohort_analysis <- function() {
  cat("Starting Cohort Analysis...\n")
  
  # 1. Connect to database and extract data
  cat("Connecting to Snowflake and extracting data...\n")
  conn <- connect_to_snowflake()
  data <- extract_customer_data(conn)
  dbDisconnect(conn)
  
  cat(paste("Extracted", nrow(data), "customer transaction records\n"))
  
  # 2. Calculate cohort metrics
  cat("Calculating cohort table...\n")
  cohort_table <- calculate_cohort_table(data)
  
  # 3. Customer lifetime value analysis
  cat("Performing CLV analysis...\n")
  customer_metrics <- calculate_clv_metrics(data)
  clv_segments <- create_clv_segmentation(customer_metrics)
  
  # 4. Survival analysis
  cat("Performing survival analysis...\n")
  survival_data <- prepare_survival_data(data)
  survival_analysis <- perform_survival_analysis(survival_data)
  
  # 5. Revenue forecasting
  cat("Generating revenue forecast...\n")
  forecast_results <- forecast_revenue(cohort_table)
  
  # 6. Create visualizations
  cat("Creating visualizations...\n")
  
  # Retention heatmap
  retention_heatmap <- create_retention_heatmap(cohort_table)
  ggsave("retention_heatmap.png", retention_heatmap, width = 12, height = 8, dpi = 300)
  
  # Dashboard plots
  dashboard_plots <- create_cohort_summary_dashboard(cohort_table, customer_metrics)
  ggsave("cohort_dashboard.png", 
         gridExtra::grid.arrange(dashboard_plots$p1, dashboard_plots$p2, 
                                dashboard_plots$p3, dashboard_plots$p4, ncol = 2),
         width = 16, height = 12, dpi = 300)
  
  # Survival plot
  ggsave("survival_analysis.png", survival_analysis$plot$plot, width = 12, height = 8, dpi = 300)
  
  # Forecast plot
  ggsave("revenue_forecast.png", forecast_results$plot, width = 12, height = 6, dpi = 300)
  
  # 7. Generate report
  cat("Generating analytical report...\n")
  report <- generate_cohort_report(cohort_table, customer_metrics, survival_analysis)
  
  # 8. Export results
  cat("Exporting results to Excel...\n")
  export_results_to_excel(cohort_table, customer_metrics, clv_segments, report)
  
  # 9. Print summary
  cat("\n" %+% "="*60 %+% "\n")
  cat("COHORT ANALYSIS SUMMARY\n")
  cat("="*60 %+% "\n")
  
  print(report$summary_stats)
  
  cat("\nKEY INSIGHTS:\n")
  for(insight in report$key_insights) {
    cat(paste("-", insight, "\n"))
  }
  
  cat("\nRECOMMENDATIONS:\n")
  for(recommendation in report$recommendations) {
    cat(paste("-", recommendation, "\n"))
  }
  
  cat("\nAnalysis completed successfully!\n")
  cat("Files generated:\n")
  cat("- retention_heatmap.png\n")
  cat("- cohort_dashboard.png\n")
  cat("- survival_analysis.png\n")
  cat("- revenue_forecast.png\n")
  cat("- cohort_analysis_results.xlsx\n")
  
  return(list(
    cohort_table = cohort_table,
    customer_metrics = customer_metrics,
    clv_segments = clv_segments,
    survival_analysis = survival_analysis,
    forecast_results = forecast_results,
    report = report
  ))
}

# ===================================================================
# Execute Analysis
# ===================================================================

if (interactive()) {
  results <- main_cohort_analysis()
} else {
  # Run when script is executed directly
  results <- main_cohort_analysis()
}
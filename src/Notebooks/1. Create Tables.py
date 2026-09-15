# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS churn_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS churn_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS churn_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS churn_catalog.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS churn_catalog.bronze.churn_volume;
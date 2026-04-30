# Local Development Guide

## Overview

This guide explains how to run the Multi-Cloud Retail Analytics Data Platform locally.

The local environment uses Docker Compose to run the core infrastructure services.

## Local Services

| Service | Purpose | URL |
|---|---|---|
| PostgreSQL | Local warehouse simulation and Airflow metadata database | localhost:5432 |
| MinIO | S3-compatible object storage | http://localhost:9001 |
| Airflow Webserver | Workflow orchestration UI | http://localhost:8080 |

## Prerequisites

Install:

- Docker
- Docker Compose
- Python 3.11+

## Environment Setup

Copy the example environment file:

```bash
cp .env.example .env
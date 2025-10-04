**Talbot Data Masking Solution**
**Overview**
A comprehensive SQL Server-based data masking solution that automatically anonymizes sensitive data across multiple databases while maintaining referential integrity and auditability.

**Architecture**
Core Components
DataMasking Database - Central repository for configuration and audit tracking

SQL Server Agent Job - Orchestrates the entire masking process

Stored Procedures - Core logic for parallel processing and data masking

Configuration Tables - Define what data to mask and how

**Key Features**
Parallel Processing: Uses thread-based execution for performance

Comprehensive Auditing: Tracks all masking operations

Constraint Handling: Automatically manages foreign keys, triggers, and CDC

Multiple Data Types: Supports strings, integers, dates, and JSON data

Non-Destructive: Preserves data format and relationships

Database Schema
Configuration Tables
DataMaskingInfo
Stores target columns and masking rules:

DatabaseName, SchemaName, TableName, ColumnName

DataType, PrimaryKeyColumn, ThreadID

IsJSONColumn, JSONColumnKey (for JSON field masking)

**Constraints**
Tracks database constraints that need temporary disabling:

Foreign keys, triggers, and CDC configurations

**DataMaskingAudit**
Logs all masking operations with timestamps and record counts

Installation & Setup
Prerequisites
SQL Server with SQL Server Agent

Appropriate database permissions

Target databases must be accessible

**Deployment**
The solution is deployed as a SQL Server Agent job (TalbotDataMasking) that executes the following steps:

Setup DataMaskingDB - Creates the central database and tables

Setup Stored Procedures - Deploys core masking logic

Load Configuration - Populates target columns and rules

Constraint Management - Handles database constraints

Execute Masking - Runs masking across all target databases

Re-enable Constraints - Restores database integrity

**Masking Strategies**
String Data (varchar/nvarchar)
Preserves first 3 characters

Appends "XXXX_" + hashed suffix

Excludes specific values ('INVALID', 'UNCODED')

Integer Data
Replaces with random numeric values

Maintains numeric format

Date/DateTime
Shifts dates backward by random days (up to 365)

Maintains temporal relationships

JSON Data
Masks specific JSON keys while preserving structure

Uses alphanumeric pattern replacement

Usage
Manual Execution
sql
-- Execute for specific database
USE DataMasking;
EXEC dbo.DataMaskingOrchestrator @DBName = 'YourDatabase';
Scheduled Execution
The solution runs as a scheduled SQL Server Agent job that can be:

Manually triggered

Scheduled for regular execution

Monitored through job history

Supported Databases
The solution currently masks data in:

Talbot_PSA


Monitoring & Troubleshooting
Audit Trail
Check DataMasking.dbo.DataMaskingAudit for:

Execution timestamps

Records processed per operation

Any error messages

Job Monitoring
Monitor TalbotDataMasking job in SQL Server Agent

Check step-level execution history

Review job step outputs for errors

Common Issues
Permission errors: Ensure service account has appropriate permissions

Constraint conflicts: Verify constraint handling completed successfully

JSON masking failures: Check JSON structure and key names

Security Considerations
Runs under SQL Server Agent service account context

Requires db_owner or similar privileges on target databases

Audit trail provides compliance reporting

No data leaves the database environment

Maintenance
Adding New Columns to Mask
Insert new records into DataMasking.dbo.DataMaskingInfo:

sql
INSERT INTO DataMasking.dbo.DataMaskingInfo
VALUES ('Database', 'Schema', 'Table', 'Column', 'DataType', 'PrimaryKey', ThreadID, IsJSON, JSONKey);
Updating Masking Rules
Modify the DataMaskingProcedure stored procedure for different masking patterns.

Performance Tuning
Adjust ThreadID values for load distribution

Monitor execution times in audit table

Consider indexing on configuration tables for large deployments

Compliance
Provides audit trail for data protection compliance

Maintains data utility while protecting sensitive information

Supports GDPR and similar regulatory requirements

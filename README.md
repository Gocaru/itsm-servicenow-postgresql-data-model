ITSM Data Model – PostgreSQL & ServiceNow Concepts

This repository contains an academic project focused on ITSM data modelling, normalization (3NF), and SQL implementation in PostgreSQL, developed within the context of a ServiceNow training program.

🎯 Objective

Transform a flat CSV dataset representing ITSM incidents into a fully normalized relational database structure.

The project emphasizes:

Third Normal Form (3NF)

Referential integrity

Business rules enforcement

Query optimization

Multi-table transactions

🧩 Dataset Characteristics

The source dataset (ITSM.csv) contains mixed operational data, including:

Incidents

Configuration Items (CI)

Categories and states

Severity indicators (Impact, Urgency, Priority)

Knowledge Base references

Operational metrics

The raw format leads to redundancy and implicit dependencies.

🏗 Design Approach

A staging strategy was adopted:

Import raw CSV into a staging table

Extract dimensions using DISTINCT

Identify entities and relationships

Apply normalization rules

Enforce ITSM business logic

🧠 ITSM Business Rule Implemented

A core ITSM principle was modeled:

Impact + Urgency → Priority


Priority is derived through a dedicated matrix (priority_matrix), reflecting real-world ITSM systems such as ServiceNow.

⚙ Database Features

The PostgreSQL implementation includes:

✔ Primary Keys
✔ Foreign Keys
✔ UNIQUE constraints
✔ Composite integrity constraints
✔ Indexes for performance
✔ Multi-table transaction example

🔁 Multi-Table Transaction Example

A transaction simulates the closure of an incident:

Update incident status

Record status history

Update related Configuration Item

This reflects realistic ITSM lifecycle operations.

🛠 Technologies Used

PostgreSQL • SQL • Data Modelling • ITSM Concepts • ServiceNow Logic

🚀 How to Execute

Run 01_itsm_structure.sql

Import CSV into stg_itsm

Run 02_itsm_import.sql

Run 03_itsm_queries_and_transaction.sql

📌 Academic Context

Developed as part of a ServiceNow-oriented training program, focusing on relational modelling and ITSM data structures.
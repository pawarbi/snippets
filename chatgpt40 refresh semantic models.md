Below is an overview of the main ways to refresh *Power BI semantic models* (often simply called “datasets” or “models”) as part of Microsoft Fabric. Since Microsoft Fabric integrates Power BI, Data Factory, Synapse, and other services into a unified analytics platform, the same refresh methods used for Power BI also apply when working with semantic models within Fabric. In addition, references to official Microsoft documentation are provided at the end.

---

## 1. Scheduled Refresh

- **Description**  
  Configure an automatic refresh schedule for your semantic model (dataset) in the Power BI (Fabric) Service. You can specify frequency (e.g. daily, hourly) and times for refresh to ensure your model data is consistently up to date.

- **Use Cases**  
  - Regularly refreshing data once a day or multiple times a day.  
  - Keeping a data model in sync without manual intervention.

- **Key Steps**  
  1. In the Fabric (Power BI) Service, go to your workspace.  
  2. Select the dataset, then choose **Settings** → **Scheduled refresh**.  
  3. Configure refresh frequency and time window.

---

## 2. On-Demand (Manual) Refresh

- **Description**  
  Initiate a refresh manually within Fabric (Power BI) at any time. This is often done via the workspace or dataset settings page.

- **Use Cases**  
  - Testing a newly published dataset.  
  - One-time refresh outside the scheduled window.  
  - Verifying data after making changes to the model.

- **Key Steps**  
  1. In the Fabric (Power BI) Service, locate your dataset in a workspace.  
  2. Click **Refresh now** from the dataset’s context menu.  
  3. Monitor refresh status in **Refresh history**.

---

## 3. Power BI REST API Refresh

- **Description**  
  Programmatically trigger a dataset refresh using the [Power BI REST APIs](https://learn.microsoft.com/power-bi/developer/rest-api). This allows developers to integrate refresh logic into custom applications, CI/CD pipelines, or other automated workflows.

- **Use Cases**  
  - Integrating refresh calls in an external scheduling tool (e.g., Azure Functions, Azure DevOps).  
  - Event-based refresh triggers (e.g., data is loaded into a source system, then an API call triggers the dataset refresh).

- **Key Steps**  
  1. Register an Azure Active Directory application with permissions to call Power BI APIs.  
  2. Obtain an OAuth2 token and use it to call the **Refresh Dataset In Group** endpoint.  
  3. Optionally monitor the refresh operation using the **Get Refresh History** endpoint.

---

## 4. XMLA Endpoint / Tabular Model Scripting Language (TMSL)

- **Description**  
  Models in Microsoft Fabric (Power BI Premium or Fabric capacities) can be accessed through XMLA endpoints, letting you use Analysis Services client tools or scripts (TMSL) to process/refresh partitions.

- **Use Cases**  
  - Advanced partition management or custom refresh strategies.  
  - Incremental refresh with custom logic.  
  - Integrations with SQL Server Management Studio (SSMS), Azure Analysis Services tools, or Tabular Editor.

- **Key Steps**  
  1. Enable the XMLA endpoint for your Power BI (Fabric) workspace.  
  2. Use SSMS/Tabular Editor to connect to the dataset.  
  3. Run TMSL commands (e.g. process partitions, process data) to refresh as needed.

---

## 5. Power Automate Flows

- **Description**  
  Use Power Automate to orchestrate data operations, including triggering a dataset refresh in Microsoft Fabric (Power BI).

- **Use Cases**  
  - Automating an end-to-end workflow that includes data updates followed by a semantic model refresh.  
  - Non-developer-friendly approach to building refresh logic.

- **Key Steps**  
  1. Create a flow in Power Automate.  
  2. Select the **Power BI** connector and use the **Refresh a dataset** action.  
  3. Configure triggers (e.g. schedule, HTTP request, or other events).

---

## 6. Data Factory/Synapse Pipelines in Fabric

- **Description**  
  Microsoft Fabric integrates Data Factory and Synapse capabilities. You can build pipelines that ingest data and then call the Power BI API (or a built-in activity) to refresh your dataset as part of an end-to-end ELT/ETL pipeline.

- **Use Cases**  
  - Orchestrating a complete pipeline that copies data from sources, transforms it, and then refreshes the model when data loading completes.  
  - Minimizing manual refresh or separate scheduling tools.

- **Key Steps**  
  1. Create or edit a pipeline in Microsoft Fabric’s Data Factory or Synapse environment.  
  2. After data ingestion/transformation steps, add an **Activity** to refresh your Fabric (Power BI) dataset (either via a built-in connector or a web activity calling the REST API).  
  3. Monitor pipeline runs and confirm refresh completion.

---

## 7. Incremental Refresh

- **Description**  
  Incremental refresh allows large datasets to be processed more efficiently by only refreshing newly added or changed data partitions. This reduces refresh time and resource usage.

- **Use Cases**  
  - Very large fact tables or historical data that rarely changes.  
  - Reducing refresh times or resource consumption for enterprise-scale data volumes.

- **Key Steps**  
  1. Configure incremental refresh in Power BI Desktop (on the table’s **Incremental Refresh** settings).  
  2. Publish the dataset to Microsoft Fabric.  
  3. Use scheduled or API-based refresh to process only new/changed partitions.

---

## 8. Deployment Pipelines (CI/CD)

- **Description**  
  Deployment pipelines in Microsoft Fabric (Power BI) facilitate automatic deployment of datasets, reports, and dataflows across dev, test, and production environments. Part of that process can include dataset refresh actions.

- **Use Cases**  
  - Managing multiple stages/environments for enterprise solutions.  
  - Automating refreshes during deployments to ensure up-to-date data.

- **Key Steps**  
  1. Create or configure a deployment pipeline in the Fabric (Power BI) Service.  
  2. Define release steps and dataset refresh actions as part of the pipeline.  
  3. Promote artifacts (datasets, reports) from Development → Test → Production, with optional refresh at each stage.

---

# References

Below are direct links and references to Microsoft’s official documentation and relevant articles about data refresh and semantic models in Fabric/Power BI:

1. **Microsoft Fabric Documentation**  
   - [Microsoft Fabric docs overview](https://learn.microsoft.com/fabric)  
   - [Manage data pipelines in Microsoft Fabric](https://learn.microsoft.com/fabric/data-factory/)  
   - [Using notebooks, SQL, and pipelines in Microsoft Fabric](https://learn.microsoft.com/fabric/synapse/)

2. **Power BI Data Refresh**  
   - [Data refresh in Power BI](https://learn.microsoft.com/power-bi/connect-data/refresh-data)  
   - [Scheduled refresh settings](https://learn.microsoft.com/power-bi/collaborate-share/service-gateway-scheduled-refresh)  
   - [Power BI dataset refresh with the REST API](https://learn.microsoft.com/power-bi/developer/automation/refresh-dataset)

3. **XMLA Endpoints (for Premium / Fabric capacities)**  
   - [XMLA read/write endpoints in Power BI](https://learn.microsoft.com/power-bi/admin/service-premium-xmla-endpoint)  
   - [Tabular Model Scripting Language (TMSL)](https://learn.microsoft.com/analysis-services/tmsl)  

4. **Incremental Refresh**  
   - [Incremental refresh in Power BI](https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview)  

5. **Power Automate**  
   - [Use the Power BI REST API in Power Automate](https://learn.microsoft.com/power-bi/developer/automation/using-flow)  

6. **Deployment Pipelines**  
   - [Deployment pipelines in Power BI](https://learn.microsoft.com/power-bi/create-reports/deployment-pipelines-overview)

7. **Git Integration for Microsoft Fabric**  
   - [Git integration in Microsoft Fabric](https://learn.microsoft.com/fabric/get-started/git-integration)  
   (While not strictly for “refresh,” Git integration can be part of CI/CD strategies that include refresh steps.)

By combining the above refresh methods with the references provided, you can create a robust strategy to keep your semantic models (datasets) up to date and integrated into larger data workflows within Microsoft Fabric.

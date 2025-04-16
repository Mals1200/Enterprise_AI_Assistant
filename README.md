# Azure Resources Used:

## Resource Group
- **Name:** enterprise_resourcegroup

## OpenAI Resources
- **OpenAI Hub:** enterprise-ai-hub
- **OpenAI Resource:** enterprise-openai-resource (currently using another resource due to quota limitations)
  - **LLM Endpoint:** gpt-o4
  - **Embedding:** 
    - text-embedding-3-large
    - text-embedding-3-ada-002

## Storage Accounts (for testing)
- **enterisesd1**
- **enterisesd2**
- **enterisesd3**

## AI Search Resource
- **Name:** enterprise-aisearch
  - **Vectors:**
    - vector-174(...)sd1
    - vector-174(...)sd2
    - vector-174(...)sd3

## Directory Hierarchy Structure for Each Storage Account:
Each Storage Account has a similar directory Hierarchy structure:
<Storage_Account>
  |__data
     |__ Tabular
     |  |__<Tables>
     |__Textual
     |  |__<Files>     <-------- (For Vectorizing)
     |__RBAC
       |__User_rbac.xlsx
       |__File_rbac.xlsx           

# ADF Analyzer v10 — Advanced Visual Architecture Documentation

**Enterprise Azure Data Factory Analysis System**  
**Version:** 10.0 Complete Edition  
**Document Date:** January 19, 2026

---

## 📊 SECTION 1: SYSTEM OVERVIEW DIAGRAMS

### 1.1 System Architecture Block Diagram

```mermaid
block-beta
    columns 5
    
    space:5
    
    block:input:1
        A["🔷 ARM Template"]
    end
    
    block:config:1
        B["⚙️ Config"]
    end
    
    block:processing:2
        C["🔧 Analyzer Engine"]
    end
    
    block:output:1
        D["📊 Excel"]
    end
    
    space:5
    
    A --> C
    B --> C
    C --> D
```

### 1.2 Complete System Flow Architecture

```mermaid
flowchart TB
    subgraph INPUT["📥 INPUT LAYER"]
        direction LR
        ARM[("🔷 ARM Template<br/>JSON Export")]
        CFG[("⚙️ Config<br/>JSON")]
    end

    subgraph ORCHESTRATION["🎛️ ORCHESTRATION LAYER"]
        direction LR
        CLI["🖥️ CLI Runner"]
        WRAPPER["🔒 UTF-8 Wrapper"]
    end

    subgraph EXTENSION["🔌 EXTENSION LAYER"]
        direction LR
        FPATCH["🧩 Functional<br/>Patches"]
        EPATCH["🎨 Excel<br/>Enhancements"]
    end

    subgraph CORE["⚙️ CORE ENGINE"]
        direction TB
        LOAD["📂 Load"]
        REGISTER["📋 Register"]
        PARSE["🔍 Parse"]
        GRAPH["🕸️ Graph"]
        ANALYZE["📈 Analyze"]
        EXPORT["💾 Export"]
        
        LOAD --> REGISTER
        REGISTER --> PARSE
        PARSE --> GRAPH
        GRAPH --> ANALYZE
        ANALYZE --> EXPORT
    end

    subgraph OUTPUT["📤 OUTPUT LAYER"]
        direction LR
        EXCEL[("📊 Excel<br/>Workbook")]
        ARCHIVE[("🗄️ Archive<br/>Copy")]
    end

    ARM --> CLI
    CFG --> CLI
    CLI --> WRAPPER
    WRAPPER --> FPATCH
    FPATCH --> EPATCH
    EPATCH --> LOAD
    EXPORT --> EXCEL
    EXCEL --> ARCHIVE

    style INPUT fill:#E3F2FD,stroke:#1976D2,stroke-width:2px
    style ORCHESTRATION fill:#FFF3E0,stroke:#FF9800,stroke-width:2px
    style EXTENSION fill:#E8F5E9,stroke:#4CAF50,stroke-width:2px
    style CORE fill:#FCE4EC,stroke:#E91E63,stroke-width:2px
    style OUTPUT fill:#F3E5F5,stroke:#9C27B0,stroke-width:2px
```

---

## 📊 SECTION 2: CORE ENGINE INTERNAL ARCHITECTURE

### 2.1 Eight-Phase Processing Pipeline

```mermaid
flowchart LR
    subgraph P1["Phase 1"]
        L["📂<br/>LOAD"]
    end
    
    subgraph P2["Phase 2"]
        R["📋<br/>REGISTER"]
    end
    
    subgraph P3["Phase 3"]
        PA["🔍<br/>PARSE"]
    end
    
    subgraph P4["Phase 4"]
        D["🔗<br/>DEPEND"]
    end
    
    subgraph P5["Phase 5"]
        G["🕸️<br/>GRAPH"]
    end
    
    subgraph P6["Phase 6"]
        T["📊<br/>TOPO"]
    end
    
    subgraph P7["Phase 7"]
        A["📈<br/>STATS"]
    end
    
    subgraph P8["Phase 8"]
        E["💾<br/>EXPORT"]
    end

    P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7 --> P8

    style P1 fill:#BBDEFB,stroke:#1976D2,stroke-width:3px
    style P2 fill:#B3E5FC,stroke:#0288D1,stroke-width:3px
    style P3 fill:#B2EBF2,stroke:#0097A7,stroke-width:3px
    style P4 fill:#B2DFDB,stroke:#00796B,stroke-width:3px
    style P5 fill:#C8E6C9,stroke:#388E3C,stroke-width:3px
    style P6 fill:#DCEDC8,stroke:#689F38,stroke-width:3px
    style P7 fill:#FFF9C4,stroke:#FBC02D,stroke-width:3px
    style P8 fill:#FFCCBC,stroke:#E64A19,stroke-width:3px
```

### 2.2 Resource Parsing Order Hierarchy

```mermaid
flowchart TD
    subgraph LAYER1["🔵 FOUNDATION LAYER"]
        IR["Integration<br/>Runtimes"]
        VNET["Managed<br/>VNets"]
    end

    subgraph LAYER2["🟢 CONNECTION LAYER"]
        LS["Linked<br/>Services"]
    end

    subgraph LAYER3["🟡 DATA LAYER"]
        DS["Datasets"]
        DF["DataFlows"]
    end

    subgraph LAYER4["🔴 EXECUTION LAYER"]
        PL["Pipelines"]
        ACT["Activities"]
    end

    subgraph LAYER5["🟣 TRIGGER LAYER"]
        TR["Triggers"]
        CR["Credentials"]
        PE["Private<br/>Endpoints"]
    end

    IR --> LS
    VNET --> LS
    LS --> DS
    LS --> DF
    DS --> PL
    DF --> PL
    PL --> ACT
    ACT --> TR
    TR --> CR
    CR --> PE

    style LAYER1 fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
    style LAYER2 fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style LAYER3 fill:#FFFDE7,stroke:#F9A825,stroke-width:3px
    style LAYER4 fill:#FFEBEE,stroke:#C62828,stroke-width:3px
    style LAYER5 fill:#F3E5F5,stroke:#7B1FA2,stroke-width:3px
```

### 2.3 Data Structure State Machine

```mermaid
stateDiagram-v2
    [*] --> Empty: Initialize
    
    Empty --> Loading: load_template()
    Loading --> Loaded: Success
    Loading --> Error: Failure
    
    Loaded --> Registering: register_resources()
    Registering --> Registered: Success
    
    Registered --> Parsing: parse_resources()
    Parsing --> Parsed: Success
    
    Parsed --> Building: extract_dependencies()
    Building --> GraphBuilt: Success
    
    GraphBuilt --> Analyzing: analyze()
    Analyzing --> Analyzed: Success
    
    Analyzed --> Exporting: export_to_excel()
    Exporting --> Complete: Success
    Exporting --> Error: Failure
    
    Complete --> [*]
    Error --> [*]
    
    note right of Loading: Validate Schema
    note right of Parsing: Recursive Activities
    note right of Building: 10+ Dependency Types
    note right of Analyzing: Cycles + Orphans
```

---

## 📊 SECTION 3: DEPENDENCY GRAPH ARCHITECTURE

### 3.1 Ten Dependency Types Visualization

```mermaid
flowchart TB
    subgraph TYPES["🔗 DEPENDENCY TYPES"]
        direction TB
        
        subgraph ACTIVITY["Activity Level"]
            T1["1️⃣ activity → activity"]
            T2["2️⃣ activity → dataset"]
        end
        
        subgraph PIPELINE["Pipeline Level"]
            T3["3️⃣ pipeline → pipeline"]
            T4["4️⃣ pipeline → dataflow"]
        end
        
        subgraph TRIGGER["Trigger Level"]
            T5["5️⃣ trigger → pipeline"]
        end
        
        subgraph DATAFLOW["DataFlow Level"]
            T6["6️⃣ dataflow → dataset"]
            T7["7️⃣ dataflow → linkedservice"]
        end
        
        subgraph RESOURCE["Resource Level"]
            T8["8️⃣ dataset → linkedservice"]
            T9["9️⃣ linkedservice → ir"]
            T10["🔟 arm_depends_on"]
        end
    end

    style ACTIVITY fill:#E3F2FD,stroke:#1976D2,stroke-width:2px
    style PIPELINE fill:#E8F5E9,stroke:#4CAF50,stroke-width:2px
    style TRIGGER fill:#FFF3E0,stroke:#FF9800,stroke-width:2px
    style DATAFLOW fill:#FCE4EC,stroke:#E91E63,stroke-width:2px
    style RESOURCE fill:#F3E5F5,stroke:#9C27B0,stroke-width:2px
```

### 3.2 Full Resource Dependency Network

```mermaid
flowchart LR
    subgraph TRIGGERS["⏰ TRIGGERS"]
        T1((T1))
        T2((T2))
    end

    subgraph PIPELINES["🔄 PIPELINES"]
        P1((P1))
        P2((P2))
        P3((P3))
    end

    subgraph ACTIVITIES["⚡ ACTIVITIES"]
        A1((A1))
        A2((A2))
        A3((A3))
        A4((A4))
    end

    subgraph DATAFLOWS["💧 DATAFLOWS"]
        DF1((DF1))
        DF2((DF2))
    end

    subgraph DATASETS["📊 DATASETS"]
        DS1((DS1))
        DS2((DS2))
        DS3((DS3))
    end

    subgraph LINKEDSERVICES["🔗 LINKED SERVICES"]
        LS1((LS1))
        LS2((LS2))
    end

    subgraph RUNTIMES["🖥️ INTEGRATION RUNTIMES"]
        IR1((IR1))
        IR2((IR2))
    end

    T1 --> P1
    T2 --> P2
    
    P1 --> A1
    P1 --> A2
    P2 --> A3
    P3 --> A4
    
    A1 --> A2
    A2 --> DF1
    A3 --> DS1
    A4 --> P3
    
    DF1 --> DS1
    DF1 --> DS2
    DF2 --> DS3
    
    DS1 --> LS1
    DS2 --> LS1
    DS3 --> LS2
    
    LS1 --> IR1
    LS2 --> IR2

    style TRIGGERS fill:#FFECB3,stroke:#FF8F00,stroke-width:2px
    style PIPELINES fill:#FFCDD2,stroke:#D32F2F,stroke-width:2px
    style ACTIVITIES fill:#BBDEFB,stroke:#1976D2,stroke-width:2px
    style DATAFLOWS fill:#C8E6C9,stroke:#388E3C,stroke-width:2px
    style DATASETS fill:#D1C4E9,stroke:#512DA8,stroke-width:2px
    style LINKEDSERVICES fill:#B2EBF2,stroke:#0097A7,stroke-width:2px
    style RUNTIMES fill:#F5F5F5,stroke:#616161,stroke-width:2px
```

---

## 📊 SECTION 4: TOPOLOGICAL EXECUTION ORDERING

### 4.1 BFS Algorithm Flow

```mermaid
flowchart TD
    START(("🚀 START")) --> INIT["Initialize<br/>in_degree map"]
    INIT --> QUEUE["Queue activities<br/>with in_degree = 0"]
    QUEUE --> CHECK{"Queue<br/>empty?"}
    
    CHECK -->|No| DEQUEUE["Dequeue<br/>activity"]
    DEQUEUE --> ASSIGN["Assign<br/>ExecutionStage"]
    ASSIGN --> PROCESS["Process<br/>neighbors"]
    PROCESS --> DECREMENT["Decrement<br/>in_degree"]
    DECREMENT --> ZERO{"in_degree<br/>= 0?"}
    
    ZERO -->|Yes| ENQUEUE["Enqueue with<br/>stage + 1"]
    ZERO -->|No| CHECK
    ENQUEUE --> CHECK
    
    CHECK -->|Yes| UPDATE["Update activity<br/>records"]
    UPDATE --> CYCLES["Mark remaining<br/>as cycles"]
    CYCLES --> FINISH(("✅ END"))

    style START fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style FINISH fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style CHECK fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style ZERO fill:#FFC107,stroke:#FF8F00,stroke-width:2px
```

### 4.2 Execution Stage Levels Visualization

```mermaid
flowchart TB
    subgraph STAGE0["🟢 STAGE 0 — No Dependencies"]
        direction LR
        S0A["Lookup1"]
        S0B["Lookup2"]
        S0C["GetParams"]
    end

    subgraph STAGE1["🟡 STAGE 1"]
        direction LR
        S1A["ForEach1"]
        S1B["ForEach2"]
    end

    subgraph STAGE2["🟠 STAGE 2"]
        direction LR
        S2A["Copy1"]
        S2B["Copy2"]
        S2C["IfCondition"]
    end

    subgraph STAGE3["🔴 STAGE 3"]
        direction LR
        S3A["Transform1"]
        S3B["Transform2"]
        S3C["StoredProc"]
    end

    subgraph STAGE4["🟣 STAGE 4 — Final"]
        direction LR
        S4A["Wait"]
        S4B["Complete"]
    end

    S0A --> S1A
    S0B --> S1A
    S0C --> S1B
    
    S1A --> S2A
    S1A --> S2B
    S1B --> S2C
    
    S2A --> S3A
    S2B --> S3B
    S2C --> S3C
    
    S3A --> S4A
    S3B --> S4A
    S3C --> S4B

    style STAGE0 fill:#C8E6C9,stroke:#2E7D32,stroke-width:3px
    style STAGE1 fill:#FFF9C4,stroke:#F9A825,stroke-width:3px
    style STAGE2 fill:#FFE0B2,stroke:#EF6C00,stroke-width:3px
    style STAGE3 fill:#FFCDD2,stroke:#C62828,stroke-width:3px
    style STAGE4 fill:#E1BEE7,stroke:#7B1FA2,stroke-width:3px
```

---

## 📊 SECTION 5: RECURSIVE ACTIVITY PARSING

### 5.1 Nested Container Structure

```mermaid
flowchart TD
    subgraph PIPELINE["📦 Pipeline: pl_Master"]
        subgraph DEPTH0["Depth 0"]
            D0A["🔍 Lookup<br/>seq=0"]
            D0B["🔁 ForEach<br/>seq=1"]
            D0C["⏳ Wait<br/>seq=6"]
        end
        
        subgraph DEPTH1["Depth 1 — Inside ForEach"]
            D1A["📋 Copy1<br/>seq=2"]
            D1B["❓ IfCondition<br/>seq=3"]
        end
        
        subgraph DEPTH2["Depth 2 — Inside If"]
            subgraph TRUE["✅ ifTrue"]
                D2A["📋 Copy2<br/>seq=4"]
            end
            subgraph FALSE["❌ ifFalse"]
                D2B["📋 Copy3<br/>seq=5"]
            end
        end
    end

    D0A --> D0B
    D0B --> D1A
    D0B --> D1B
    D1B --> D2A
    D1B --> D2B
    D1A --> D0C
    D2A --> D0C
    D2B --> D0C

    style DEPTH0 fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
    style DEPTH1 fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style DEPTH2 fill:#FFF8E1,stroke:#FF8F00,stroke-width:3px
    style TRUE fill:#C8E6C9,stroke:#388E3C,stroke-width:2px
    style FALSE fill:#FFCDD2,stroke:#D32F2F,stroke-width:2px
```

### 5.2 Container Type Dispatch

```mermaid
flowchart TD
    PARSE["Parse Activity"] --> TYPE{"Activity<br/>Type?"}
    
    TYPE -->|ForEach| FE["Get activities array<br/>Recurse with depth+1"]
    TYPE -->|IfCondition| IF["Get ifTrue & ifFalse<br/>Recurse both paths"]
    TYPE -->|Switch| SW["Get cases & default<br/>Recurse each case"]
    TYPE -->|Until| UN["Get activities array<br/>Recurse with depth+1"]
    TYPE -->|Other| OTHER["Parse properties<br/>Add to results"]
    
    FE --> RECURSE["🔄 Recursive Call"]
    IF --> RECURSE
    SW --> RECURSE
    UN --> RECURSE
    OTHER --> DONE["✅ Complete"]
    RECURSE --> DONE

    style TYPE fill:#FFC107,stroke:#FF8F00,stroke-width:3px
    style RECURSE fill:#2196F3,stroke:#1565C0,stroke-width:3px,color:#fff
    style DONE fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
```

---

## 📊 SECTION 6: MONKEY PATCHING ARCHITECTURE

### 6.1 Patch Injection Sequence

```mermaid
sequenceDiagram
    autonumber
    
    participant R as 🖥️ Runner
    participant P as 🧩 Patch Module
    participant C as 📦 Analyzer Class
    
    rect rgb(227, 242, 253)
        Note over R,C: Phase 1: Import & Prepare
        R->>P: Import patch module
        R->>P: Call apply_all_patches()
        P->>C: Import analyzer class
    end
    
    rect rgb(232, 245, 233)
        Note over P,C: Phase 2: Inject Parsers
        P->>C: Inject Databricks parser
        P->>C: Inject AzureFunction parser
        P->>C: Inject HDInsight parser
        P->>C: Inject Salesforce parser
    end
    
    rect rgb(255, 243, 224)
        Note over P,C: Phase 3: Override Dispatcher
        P->>C: Save original parse_activity
        P->>C: Replace with enhanced dispatcher
    end
    
    rect rgb(252, 228, 236)
        Note over P,C: Phase 4: Enhance Datasets
        P->>C: Inject dataset location enhancer
    end
    
    P-->>R: ✅ Patching complete
    
    rect rgb(243, 229, 245)
        Note over R,C: Phase 5: Instantiate
        R->>C: Create analyzer instance
        Note over C: All patches active
    end
```

### 6.2 Before vs After Patching

```mermaid
flowchart LR
    subgraph BEFORE["🔵 BEFORE PATCHING"]
        direction TB
        B1["Base Class"]
        B2["19 Parsers"]
        B3["Original Dispatcher"]
    end

    subgraph PATCH["🟢 PATCH PROCESS"]
        direction TB
        P1["+ Databricks"]
        P2["+ AzureFunction"]
        P3["+ HDInsight"]
        P4["+ Salesforce"]
        P5["+ Dataset Enhance"]
        P6["Override Dispatcher"]
    end

    subgraph AFTER["🟣 AFTER PATCHING"]
        direction TB
        A1["Enhanced Class"]
        A2["26 Parsers"]
        A3["Enhanced Dispatcher"]
    end

    BEFORE --> PATCH
    PATCH --> AFTER

    style BEFORE fill:#BBDEFB,stroke:#1565C0,stroke-width:3px
    style PATCH fill:#C8E6C9,stroke:#2E7D32,stroke-width:3px
    style AFTER fill:#E1BEE7,stroke:#7B1FA2,stroke-width:3px
```

### 6.3 Enhanced Dispatcher Logic

```mermaid
flowchart TD
    CALL["parse_activity() called"] --> ORIG["Call original parser"]
    ORIG --> RESULT["Get base result"]
    RESULT --> CHECK{"Check<br/>activity type"}
    
    CHECK -->|Databricks*| DB["🧱 Databricks Parser<br/>notebook, jar, python"]
    CHECK -->|AzureFunction| AF["⚡ AzureFunction Parser<br/>name, method, body"]
    CHECK -->|HDInsight*| HD["🔷 HDInsight Parser<br/>jar, class, args"]
    CHECK -->|Salesforce*| SF["☁️ Salesforce Parser<br/>soql, object"]
    CHECK -->|Other| RET["Return base result"]
    
    DB --> MERGE["Merge properties"]
    AF --> MERGE
    HD --> MERGE
    SF --> MERGE
    MERGE --> RET

    style CHECK fill:#FFC107,stroke:#FF8F00,stroke-width:3px
    style DB fill:#FF7043,stroke:#E64A19,stroke-width:2px,color:#fff
    style AF fill:#7E57C2,stroke:#512DA8,stroke-width:2px,color:#fff
    style HD fill:#42A5F5,stroke:#1976D2,stroke-width:2px,color:#fff
    style SF fill:#26C6DA,stroke:#00838F,stroke-width:2px,color:#fff
```

---

## 📊 SECTION 7: EXCEL EXPORT PIPELINE

### 7.1 Export Process Flow

```mermaid
flowchart TD
    subgraph PREPARE["📋 PREPARE"]
        P1["Convert results<br/>to DataFrames"]
        P2["Apply sorting"]
        P3["Apply column hiding"]
    end

    subgraph WRITE["💾 WRITE"]
        W1["Create Excel writer"]
        W2["Write core sheets"]
        W3["Auto-split large sheets"]
    end

    subgraph ENHANCE["🎨 ENHANCE"]
        E1["Apply styling"]
        E2["Add formatting"]
        E3["Insert hyperlinks"]
        E4["Build dashboard"]
    end

    subgraph OUTPUT["📤 OUTPUT"]
        O1["Save workbook"]
        O2["Create archive"]
        O3["Copy to Streamlit"]
    end

    P1 --> P2 --> P3 --> W1
    W1 --> W2 --> W3 --> E1
    E1 --> E2 --> E3 --> E4 --> O1
    O1 --> O2 --> O3

    style PREPARE fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
    style WRITE fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style ENHANCE fill:#FFF3E0,stroke:#EF6C00,stroke-width:3px
    style OUTPUT fill:#F3E5F5,stroke:#7B1FA2,stroke-width:3px
```

### 7.2 Auto-Split Logic for Large Sheets

```mermaid
flowchart TD
    START["Sheet data"] --> CHECK{"Rows ><br/>1,000,000?"}
    
    CHECK -->|No| SINGLE["Write single sheet"]
    CHECK -->|Yes| SPLIT["Calculate parts"]
    
    SPLIT --> LOOP["For each chunk"]
    LOOP --> WRITE["Write Sheet_P{n}"]
    WRITE --> MORE{"More<br/>chunks?"}
    
    MORE -->|Yes| LOOP
    MORE -->|No| DONE["✅ Complete"]
    SINGLE --> DONE

    style CHECK fill:#FFC107,stroke:#FF8F00,stroke-width:3px
    style MORE fill:#FFC107,stroke:#FF8F00,stroke-width:3px
    style DONE fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
```

### 7.3 Excel Sheet Organization

```mermaid
flowchart TB
    subgraph CORE["📊 CORE SHEETS"]
        direction LR
        C1["PipelineAnalysis"]
        C2["Pipelines"]
        C3["Activities ⭐"]
        C4["ActivityExecutionOrder ⭐"]
    end

    subgraph DATAFLOW["💧 DATAFLOW SHEETS"]
        direction LR
        D1["DataFlows"]
        D2["DataFlowLineage"]
        D3["DataFlowTransformations"]
    end

    subgraph RESOURCE["📦 RESOURCE SHEETS"]
        direction LR
        R1["Datasets"]
        R2["LinkedServices"]
        R3["IntegrationRuntimes"]
        R4["Triggers"]
    end

    subgraph ANALYSIS["📈 ANALYSIS SHEETS"]
        direction LR
        A1["Dependencies"]
        A2["CircularDependencies"]
        A3["ImpactAnalysis"]
        A4["DataLineage"]
    end

    subgraph ORPHAN["⚠️ ORPHAN SHEETS"]
        direction LR
        O1["OrphanedPipelines"]
        O2["OrphanedDatasets"]
        O3["OrphanedLinkedServices"]
    end

    subgraph USAGE["📊 USAGE SHEETS"]
        direction LR
        U1["DatasetUsage"]
        U2["LinkedServiceUsage"]
        U3["IntegrationRuntimeUsage"]
    end

    style CORE fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
    style DATAFLOW fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style RESOURCE fill:#FFF3E0,stroke:#EF6C00,stroke-width:3px
    style ANALYSIS fill:#FCE4EC,stroke:#C2185B,stroke-width:3px
    style ORPHAN fill:#FFF8E1,stroke:#F57F17,stroke-width:3px
    style USAGE fill:#F3E5F5,stroke:#7B1FA2,stroke-width:3px
```

---

## 📊 SECTION 8: ENHANCEMENT LAYER ARCHITECTURE

### 8.1 Enhancement Pipeline Flow

```mermaid
flowchart TD
    subgraph INPUT["📥 INPUT"]
        I1["Raw Excel<br/>from Analyzer"]
        I2["enhancement_config.json"]
    end

    subgraph STEP1["Step 1: REWRITE"]
        S1A["Read Activities"]
        S1B["Sort by Pipeline + Stage"]
        S1C["Write back"]
    end

    subgraph STEP2["Step 2: STYLE"]
        S2A["Format headers"]
        S2B["Apply borders"]
        S2C["Freeze panes"]
        S2D["Enable filters"]
    end

    subgraph STEP3["Step 3: FORMAT"]
        S3A["Data bars"]
        S3B["Color scales"]
        S3C["Icon sets"]
    end

    subgraph STEP4["Step 4: NAVIGATE"]
        S4A["Insert hyperlinks"]
        S4B["Build TOC"]
    end

    subgraph STEP5["Step 5: DASHBOARD"]
        S5A["Project banner"]
        S5B["Metrics"]
        S5C["Alerts"]
    end

    subgraph OUTPUT["📤 OUTPUT"]
        O1["Enhanced Excel"]
    end

    I1 --> S1A
    I2 --> S1A
    S1A --> S1B --> S1C --> S2A
    S2A --> S2B --> S2C --> S2D --> S3A
    S3A --> S3B --> S3C --> S4A
    S4A --> S4B --> S5A
    S5A --> S5B --> S5C --> O1

    style INPUT fill:#E3F2FD,stroke:#1565C0,stroke-width:2px
    style STEP1 fill:#FFF3E0,stroke:#EF6C00,stroke-width:2px
    style STEP2 fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px
    style STEP3 fill:#FCE4EC,stroke:#C2185B,stroke-width:2px
    style STEP4 fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px
    style STEP5 fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
    style OUTPUT fill:#DCEDC8,stroke:#689F38,stroke-width:3px
```

### 8.2 Enhancement Configuration Decision Tree

```mermaid
flowchart TD
    CONFIG["Load Config"] --> ENABLED{"enabled?"}
    
    ENABLED -->|No| SKIP["Skip all<br/>Return raw"]
    ENABLED -->|Yes| CORE{"core_formatting?"}
    
    CORE -->|Yes| APPLYSTYLE["✅ Apply styling"]
    CORE -->|No| SKIPSTYLE["⏭️ Skip styling"]
    
    APPLYSTYLE --> COND
    SKIPSTYLE --> COND
    
    COND{"conditional<br/>formatting?"}
    COND -->|Yes| APPLYFORMAT["✅ Apply formats"]
    COND -->|No| SKIPFORMAT["⏭️ Skip formats"]
    
    APPLYFORMAT --> LINKS
    SKIPFORMAT --> LINKS
    
    LINKS{"hyperlinks?"}
    LINKS -->|Yes| APPLYLINKS["✅ Apply links"]
    LINKS -->|No| SKIPLINKS["⏭️ Skip links"]
    
    APPLYLINKS --> SUMM
    SKIPLINKS --> SUMM
    
    SUMM{"enhanced<br/>summary?"}
    SUMM -->|Yes| APPLYSUMM["✅ Apply summary"]
    SUMM -->|No| SKIPSUMM["⏭️ Skip summary"]
    
    APPLYSUMM --> DONE["Output enhanced Excel"]
    SKIPSUMM --> DONE
    SKIP --> DONE

    style ENABLED fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style CORE fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style COND fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style LINKS fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style SUMM fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style DONE fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
```

---

## 📊 SECTION 9: CLI EXECUTION MODES

### 9.1 Four Execution Mode Comparison

```mermaid
flowchart TB
    subgraph MODE1["🔵 BASIC MODE<br/>--basic"]
        direction TB
        M1A["Skip functional patches"]
        M1B["Skip Excel enhancements"]
        M1C["Base analyzer only"]
        M1D["Plain Excel output"]
    end

    subgraph MODE2["🟢 FUNCTIONAL ONLY<br/>--skip-excel-enhancements"]
        direction TB
        M2A["Apply functional patches"]
        M2B["Skip Excel enhancements"]
        M2C["Extended parsers active"]
        M2D["Plain Excel output"]
    end

    subgraph MODE3["🟡 EXCEL ONLY<br/>--skip-functional"]
        direction TB
        M3A["Skip functional patches"]
        M3B["Apply Excel enhancements"]
        M3C["Base parsers only"]
        M3D["Styled Excel output"]
    end

    subgraph MODE4["🟣 FULL PRODUCTION<br/>(default)"]
        direction TB
        M4A["Apply functional patches"]
        M4B["Apply Excel enhancements"]
        M4C["All parsers active"]
        M4D["Fully enhanced Excel"]
    end

    style MODE1 fill:#BBDEFB,stroke:#1565C0,stroke-width:3px
    style MODE2 fill:#C8E6C9,stroke:#2E7D32,stroke-width:3px
    style MODE3 fill:#FFF9C4,stroke:#F9A825,stroke-width:3px
    style MODE4 fill:#E1BEE7,stroke:#7B1FA2,stroke-width:3px
```

### 9.2 CLI Decision Flow

```mermaid
flowchart TD
    START["User runs CLI"] --> PARSE["Parse arguments"]
    PARSE --> BASIC{"--basic<br/>flag?"}
    
    BASIC -->|Yes| SKIPALL["Skip all patches<br/>Skip all enhancements"]
    BASIC -->|No| CHECKF{"--skip-functional?"}
    
    CHECKF -->|Yes| SKIPFUNC["Skip functional patches"]
    CHECKF -->|No| APPLYFUNC["Apply functional patches"]
    
    SKIPFUNC --> CHECKE{"--skip-excel?"}
    APPLYFUNC --> CHECKE
    
    CHECKE -->|Yes| SKIPEXCEL["Skip Excel enhancements"]
    CHECKE -->|No| APPLYEXCEL["Apply Excel enhancements"]
    
    SKIPALL --> RUN
    SKIPEXCEL --> RUN
    APPLYEXCEL --> RUN
    
    RUN["Run analyzer"] --> OUTPUT["Generate Excel"]

    style BASIC fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style CHECKF fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style CHECKE fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style OUTPUT fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
```

---

## 📊 SECTION 10: DATA LINEAGE TRACEABILITY

### 10.1 End-to-End Data Lineage Chain

```mermaid
flowchart LR
    subgraph TRIGGER["⏰ TRIGGER"]
        T["Schedule<br/>Every 15 min"]
    end

    subgraph PIPELINE1["🔄 MASTER PIPELINE"]
        P1["pl_Master"]
    end

    subgraph ACTIVITIES1["⚡ ACTIVITIES"]
        A1["Lookup"]
        A2["ExecutePipeline"]
        A3["ExecuteDataFlow"]
    end

    subgraph PIPELINE2["🔄 CHILD PIPELINE"]
        P2["pl_Child"]
    end

    subgraph ACTIVITIES2["⚡ CHILD ACTIVITIES"]
        A4["Copy"]
    end

    subgraph DATAFLOW["💧 DATAFLOW"]
        DF["df_Transform"]
    end

    subgraph DATASETS["📊 DATASETS"]
        DS1["Source"]
        DS2["Staging"]
        DS3["Target"]
    end

    subgraph LINKEDSERVICES["🔗 LINKED SERVICES"]
        LS1["ls_Source"]
        LS2["ls_Target"]
    end

    subgraph RUNTIMES["🖥️ RUNTIMES"]
        IR1["Azure IR"]
        IR2["Self-hosted IR"]
    end

    T --> P1
    P1 --> A1
    P1 --> A2
    P1 --> A3
    A2 --> P2
    P2 --> A4
    A4 --> DS1
    A4 --> DS2
    A3 --> DF
    DF --> DS2
    DF --> DS3
    DS1 --> LS1
    DS2 --> LS2
    DS3 --> LS2
    LS1 --> IR1
    LS2 --> IR2

    style TRIGGER fill:#FFECB3,stroke:#FF8F00,stroke-width:3px
    style PIPELINE1 fill:#FFCDD2,stroke:#D32F2F,stroke-width:3px
    style PIPELINE2 fill:#FFCDD2,stroke:#D32F2F,stroke-width:2px
    style DATAFLOW fill:#C8E6C9,stroke:#388E3C,stroke-width:3px
    style DATASETS fill:#D1C4E9,stroke:#512DA8,stroke-width:3px
    style LINKEDSERVICES fill:#B2EBF2,stroke:#0097A7,stroke-width:3px
    style RUNTIMES fill:#F5F5F5,stroke:#616161,stroke-width:3px
```

---

## 📊 SECTION 11: CYCLE DETECTION ALGORITHM

### 11.1 Tarjan's SCC Algorithm Flow

```mermaid
flowchart TD
    START(("🚀 START")) --> INIT["Initialize<br/>index, lowlink, stack"]
    INIT --> FORALL["For each node"]
    FORALL --> VISITED{"Node<br/>visited?"}
    
    VISITED -->|No| STRONG["strongconnect(node)"]
    VISITED -->|Yes| NEXT["Next node"]
    NEXT --> DONE{"All nodes<br/>processed?"}
    
    DONE -->|No| FORALL
    DONE -->|Yes| RESULT["Return SCC list"]
    
    STRONG --> SETINDEX["Set index, lowlink"]
    SETINDEX --> PUSH["Push to stack"]
    PUSH --> NEIGHBORS["For each neighbor"]
    NEIGHBORS --> NVISITED{"Neighbor<br/>visited?"}
    
    NVISITED -->|No| RECURSE["Recurse neighbor"]
    NVISITED -->|Yes, on stack| UPDATE["Update lowlink"]
    
    RECURSE --> UPDATEMIN["lowlink = min(...)"]
    UPDATE --> UPDATEMIN
    UPDATEMIN --> MOREN{"More<br/>neighbors?"}
    
    MOREN -->|Yes| NEIGHBORS
    MOREN -->|No| ISROOT{"Is root?<br/>lowlink = index"}
    
    ISROOT -->|Yes| POP["Pop SCC<br/>from stack"]
    ISROOT -->|No| RETURN["Return"]
    
    POP --> RECORD["Record cycle<br/>if size > 1"]
    RECORD --> RETURN
    RETURN --> FORALL

    RESULT --> FINISH(("✅ END"))

    style START fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style FINISH fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style VISITED fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style NVISITED fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style DONE fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style MOREN fill:#FFC107,stroke:#FF8F00,stroke-width:2px
    style ISROOT fill:#FFC107,stroke:#FF8F00,stroke-width:2px
```

---

## 📊 SECTION 12: COMPLETE SYSTEM SUMMARY

### 12.1 System Architecture Overview

```mermaid
flowchart TB
    subgraph USER["👤 USER"]
        CLI["python patched_runner.py<br/>template.json"]
    end

    subgraph ORCHESTRATOR["🎛️ ORCHESTRATOR"]
        RUNNER["Patched Runner"]
        ARGS["Parse Args"]
    end

    subgraph EXTENSIONS["🔌 EXTENSIONS"]
        FP["Functional Patches<br/>+7 parsers"]
        EP["Excel Enhancements<br/>styling + dashboards"]
    end

    subgraph ENGINE["⚙️ CORE ENGINE"]
        PHASE["8 Processing Phases"]
    end

    subgraph OUTPUTS["📤 OUTPUTS"]
        EXCEL["📊 Excel Workbook"]
        ARCHIVE["🗄️ Archive Copy"]
        STCOPY["📁 Streamlit Copy"]
    end

    subgraph CONSUMERS["👥 CONSUMERS"]
        MANUAL["Manual Review"]
        VALID["Validation Scripts"]
        STREAM["Streamlit Dashboard<br/>(out of scope)"]
    end

    CLI --> RUNNER
    RUNNER --> ARGS
    ARGS --> FP
    ARGS --> EP
    FP --> PHASE
    EP --> PHASE
    PHASE --> EXCEL
    EXCEL --> ARCHIVE
    EXCEL --> STCOPY
    EXCEL --> MANUAL
    EXCEL --> VALID
    STCOPY -.-> STREAM

    style USER fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
    style ORCHESTRATOR fill:#FFF3E0,stroke:#EF6C00,stroke-width:3px
    style EXTENSIONS fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style ENGINE fill:#FCE4EC,stroke:#C2185B,stroke-width:3px
    style OUTPUTS fill:#F3E5F5,stroke:#7B1FA2,stroke-width:3px
    style CONSUMERS fill:#E1F5FE,stroke:#0277BD,stroke-width:3px
```

### 12.2 File Responsibility Summary

```mermaid
flowchart LR
    subgraph FILES["📁 CORE FILES"]
        F1["adf_analyzer_v10_complete.py<br/>⚙️ Core Engine"]
        F2["adf_analyzer_v10_patch.py<br/>🧩 Extensions"]
        F3["adf_analyzer_v10_patched_runner.py<br/>🎛️ Orchestrator"]
        F4["adf_analyzer_v10_excel_enhancements.py<br/>🎨 Beautification"]
    end

    subgraph ROLES["🎯 ROLES"]
        R1["ARM parsing<br/>Dependency graphs<br/>Topological sort<br/>Excel export"]
        R2["Activity parsers<br/>Dataset parsers<br/>Dispatcher override"]
        R3["CLI handling<br/>Patch control<br/>Execution flow"]
        R4["Styling<br/>Formatting<br/>Dashboards"]
    end

    F1 --> R1
    F2 --> R2
    F3 --> R3
    F4 --> R4

    style F1 fill:#FCE4EC,stroke:#C2185B,stroke-width:3px
    style F2 fill:#E8F5E9,stroke:#2E7D32,stroke-width:3px
    style F3 fill:#FFF3E0,stroke:#EF6C00,stroke-width:3px
    style F4 fill:#E3F2FD,stroke:#1565C0,stroke-width:3px
```

---

## 📋 DIAGRAM TYPE REFERENCE

| Section | Diagram Type | Purpose |
|---------|--------------|---------|
| 1.1 | Block Diagram | High-level system view |
| 1.2 | Layered Flowchart | Component architecture |
| 2.1 | Horizontal Pipeline | Phase progression |
| 2.2 | Vertical Hierarchy | Resource dependencies |
| 2.3 | State Diagram | Processing states |
| 3.1-3.2 | Network Graph | Dependency visualization |
| 4.1 | Algorithm Flowchart | BFS logic |
| 4.2 | Staged Flowchart | Execution levels |
| 5.1-5.2 | Nested Flowchart | Recursive structure |
| 6.1 | Sequence Diagram | Runtime interaction |
| 6.2-6.3 | Transformation Flowchart | Patch mechanism |
| 7.1-7.3 | Pipeline Flowchart | Export process |
| 8.1-8.2 | Decision Tree | Configuration logic |
| 9.1-9.2 | Comparison Flowchart | Mode differences |
| 10.1 | Lineage Graph | Data traceability |
| 11.1 | Algorithm Flowchart | Tarjan's SCC |
| 12.1-12.2 | Summary Flowchart | System overview |

---

**Document Version:** 3.0 Advanced Edition  
**Diagram Syntax:** Validated Mermaid 10.x  
**Last Updated:** January 19, 2026

**The generated Excel workbook is later consumed by a Streamlit-based visualization layer, which is under active development and intentionally out of scope for this document.**

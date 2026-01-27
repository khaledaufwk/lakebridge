"""
Review scripts for Lakebridge skills.

Provides code analysis and validation capabilities:
- CodeAnalyzer: Core analysis functionality
- BusinessLogicExtractor: T-SQL and Spark logic extraction (Phase 3)
- DependencyTracker: Dependency tracking and resolution (Phase 3)
- SPChecklistGenerator: SP-specific review checklists (Phase 4)
- DataTypeValidator: Data type mapping validation (Phase 4)
- EnhancedReviewReport: SP-specific report format (Phase 4)
"""

from .analyzer import CodeAnalyzer, ReviewReport, RiskTier, Issue

# Phase 3: Business Logic Comparator (REQ-R3)
try:
    from .analyzer_business_logic import (
        BusinessLogicExtractor,
        BusinessLogicComparator,
        BusinessLogic,
        LogicComparison,
        ComparisonStatus,
        TableReference,
        ColumnReference,
        JoinInfo,
        AggregationInfo,
        FilterInfo,
    )
    PHASE3_BUSINESS_LOGIC_AVAILABLE = True
except ImportError:
    PHASE3_BUSINESS_LOGIC_AVAILABLE = False

# Phase 3: Dependency Tracker (REQ-R4)
try:
    from .analyzer_dependencies import (
        DependencyTracker,
        DependencyAnalyzer,
        Dependency,
        DependencyType,
        DependencyGraph,
    )
    PHASE3_DEPENDENCIES_AVAILABLE = True
except ImportError:
    PHASE3_DEPENDENCIES_AVAILABLE = False

# Phase 4: Checklist Generator (REQ-R5)
try:
    from .checklist_generator import (
        SPChecklistGenerator,
        SPChecklistConfig,
        ChecklistItem,
        ChecklistSection,
        QuickChecklist,
    )
    PHASE4_CHECKLIST_AVAILABLE = True
except ImportError:
    PHASE4_CHECKLIST_AVAILABLE = False

# Phase 4: Data Type Validator (REQ-R6)
try:
    from .datatype_validator import (
        DataTypeValidator,
        TypeValidationIssue,
        ValidationSeverity,
        ColumnTypeInfo,
        TSQL_TO_SPARK_MAPPINGS,
    )
    PHASE4_DATATYPE_AVAILABLE = True
except ImportError:
    PHASE4_DATATYPE_AVAILABLE = False

# Phase 4: Enhanced Report (REQ-R7)
try:
    from .enhanced_report import (
        EnhancedReviewReport,
        ReportBuilder,
        SPAnalysisResult,
        PatternConversionStatus,
        DependencyStatus,
    )
    PHASE4_REPORT_AVAILABLE = True
except ImportError:
    PHASE4_REPORT_AVAILABLE = False

__all__ = [
    # Core
    "CodeAnalyzer",
    "ReviewReport",
    "RiskTier",
    "Issue",
    # Phase 3: Business Logic (REQ-R3)
    "BusinessLogicExtractor",
    "BusinessLogicComparator",
    "BusinessLogic",
    "LogicComparison",
    "ComparisonStatus",
    "TableReference",
    "ColumnReference",
    "JoinInfo",
    "AggregationInfo",
    "FilterInfo",
    # Phase 3: Dependencies (REQ-R4)
    "DependencyTracker",
    "DependencyAnalyzer",
    "Dependency",
    "DependencyType",
    "DependencyGraph",
    # Phase 4: Checklist (REQ-R5)
    "SPChecklistGenerator",
    "SPChecklistConfig",
    "ChecklistItem",
    "ChecklistSection",
    "QuickChecklist",
    # Phase 4: Data Types (REQ-R6)
    "DataTypeValidator",
    "TypeValidationIssue",
    "ValidationSeverity",
    "ColumnTypeInfo",
    "TSQL_TO_SPARK_MAPPINGS",
    # Phase 4: Report (REQ-R7)
    "EnhancedReviewReport",
    "ReportBuilder",
    "SPAnalysisResult",
    "PatternConversionStatus",
    "DependencyStatus",
    # Availability flags
    "PHASE3_BUSINESS_LOGIC_AVAILABLE",
    "PHASE3_DEPENDENCIES_AVAILABLE",
    "PHASE4_CHECKLIST_AVAILABLE",
    "PHASE4_DATATYPE_AVAILABLE",
    "PHASE4_REPORT_AVAILABLE",
]

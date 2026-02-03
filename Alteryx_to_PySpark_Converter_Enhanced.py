# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx to PySpark Converter Tool - Enhanced Version
# MAGIC ## Dynamic Workflow Conversion with Exact Configuration Extraction
# MAGIC 
# MAGIC This notebook reads any Alteryx workflow file (.yxmd) and automatically converts it to **accurate, runnable** PySpark code.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Extracts exact column names, join keys, filter expressions
# MAGIC - Converts Alteryx formulas to PySpark expressions
# MAGIC - Tracks data flow through connections
# MAGIC - Generates container-organized code
# MAGIC - Preserves Text Input data exactly
# MAGIC 
# MAGIC **Usage:**
# MAGIC 1. Upload your .yxmd file to Databricks Volumes
# MAGIC 2. Set the `workflow_file_path` below
# MAGIC 3. Run all cells

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# CONFIGURATION - Set your workflow file path here
workflow_file_path = "/Volumes/your_catalog/your_schema/your_volume/your_workflow.yxmd"

# Output options
output_notebook_path = "/Workspace/Users/your_user/converted_workflow.py"
generate_file = True

# COMMAND ----------
# MAGIC %md
# MAGIC ## Core Libraries

# COMMAND ----------
import xml.etree.ElementTree as ET
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple, Set
import re
from datetime import datetime
import html

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Classes

# COMMAND ----------
@dataclass
class SelectField:
    """Represents a field in Select tool"""
    field_name: str
    selected: bool
    rename: Optional[str] = None
    field_type: Optional[str] = None
    size: Optional[int] = None
    
@dataclass
class FormulaField:
    """Represents a formula field"""
    field_name: str
    expression: str
    field_type: str
    size: Optional[int] = None

@dataclass
class JoinInfo:
    """Represents join configuration"""
    left_keys: List[str]
    right_keys: List[str]
    join_type: str = "inner"
    select_config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SummarizeField:
    """Represents a summarize field"""
    field_name: str
    action: str  # GroupBy, Sum, Count, Avg, Min, Max, First, Last, Concat, etc.
    rename: Optional[str] = None

@dataclass 
class TextInputData:
    """Represents Text Input tool data"""
    columns: List[str]
    rows: List[List[str]]
    
@dataclass
class AlteryxTool:
    """Represents an Alteryx tool/node with full configuration"""
    tool_id: str
    plugin: str
    tool_type: str
    position: Dict[str, int]
    annotation: str = ""
    
    # Specific configurations by tool type
    input_config: Optional[Dict] = None
    select_fields: Optional[List[SelectField]] = None
    filter_expression: Optional[str] = None
    filter_mode: Optional[str] = None
    formula_fields: Optional[List[FormulaField]] = None
    join_info: Optional[JoinInfo] = None
    summarize_fields: Optional[List[SummarizeField]] = None
    sort_fields: Optional[List[Tuple[str, str]]] = None  # (field, order)
    text_input_data: Optional[TextInputData] = None
    text_to_columns_config: Optional[Dict] = None
    crosstab_config: Optional[Dict] = None
    unique_fields: Optional[List[str]] = None
    sample_config: Optional[Dict] = None
    output_config: Optional[Dict] = None
    find_replace_config: Optional[Dict] = None
    raw_config: Optional[Dict] = None

@dataclass
class AlteryxContainer:
    """Represents an Alteryx container"""
    tool_id: str
    caption: str
    position: Dict[str, int]
    tools: List[AlteryxTool] = field(default_factory=list)
    child_containers: List['AlteryxContainer'] = field(default_factory=list)
    disabled: bool = False

@dataclass
class AlteryxConnection:
    """Represents a connection between tools"""
    origin_tool_id: str
    origin_connection: str
    destination_tool_id: str
    destination_connection: str
    name: str = ""

@dataclass 
class AlteryxWorkflow:
    """Represents the complete Alteryx workflow"""
    name: str
    version: str
    containers: List[AlteryxContainer]
    tools: List[AlteryxTool]
    connections: List[AlteryxConnection]
    constants: Dict[str, Any]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expression Converter

# COMMAND ----------
class AlteryxExpressionConverter:
    """Converts Alteryx expressions to PySpark expressions"""
    
    def __init__(self):
        # Function mappings
        self.function_map = {
            # String functions
            'Trim': 'trim',
            'TrimLeft': 'ltrim',
            'TrimRight': 'rtrim',
            'Upper': 'upper',
            'Uppercase': 'upper',
            'Lower': 'lower',
            'Lowercase': 'lower',
            'Left': 'substring({0}, 1, {1})',
            'Right': 'substring({0}, -cast({1} as int), cast({1} as int))',
            'Substring': 'substring',
            'Length': 'length',
            'Len': 'length',
            'Replace': 'regexp_replace',
            'Contains': '{0}.contains({1})',
            'StartsWith': '{0}.startswith({1})',
            'EndsWith': '{0}.endswith({1})',
            'FindString': 'locate({1}, {0})',
            'PadLeft': 'lpad',
            'PadRight': 'rpad',
            'Concat': 'concat',
            
            # Null functions
            'IsNull': '{0}.isNull()',
            'IsNotNull': '{0}.isNotNull()',
            'IsEmpty': '(({0}.isNull()) | ({0} == ""))',
            'IsNotEmpty': '(({0}.isNotNull()) & ({0} != ""))',
            'Null': 'lit(None)',
            'IFNULL': 'coalesce({0}, {1})',
            'NullIf': 'when({0} == {1}, lit(None)).otherwise({0})',
            
            # Type conversion
            'ToString': '{0}.cast("string")',
            'ToNumber': '{0}.cast("double")',
            'ToInteger': '{0}.cast("int")',
            'ToDate': 'to_date({0}, {1})',
            'ToDateTime': 'to_timestamp({0}, {1})',
            
            # Date functions
            'DateTimeNow': 'current_timestamp()',
            'DateTimeToday': 'current_date()',
            'DateTimeYear': 'year({0})',
            'DateTimeMonth': 'month({0})',
            'DateTimeDay': 'dayofmonth({0})',
            'DateTimeAdd': 'date_add({0}, {1})',
            'DateTimeDiff': 'datediff({1}, {0})',
            
            # Math functions
            'Abs': 'abs',
            'Ceil': 'ceil',
            'Floor': 'floor',
            'Round': 'round',
            'Mod': 'mod',
            'Pow': 'pow',
            'Sqrt': 'sqrt',
            'Log': 'log',
            'Log10': 'log10',
            'Exp': 'exp',
            'Rand': 'rand',
            
            # Aggregate (for formulas that use these)
            'Min': 'least',
            'Max': 'greatest',
            
            # Regex
            'REGEX_Match': 'regexp_extract({0}, {1}, 0) != ""',
            'REGEX_Replace': 'regexp_replace({0}, {1}, {2})',
            'REGEX_CountMatches': 'size(split({0}, {1})) - 1',
        }
    
    def convert(self, alteryx_expr: str) -> str:
        """Convert Alteryx expression to PySpark expression"""
        if not alteryx_expr:
            return 'lit(True)'
        
        # Unescape HTML entities
        expr = html.unescape(alteryx_expr)
        
        # Store original for comment
        original = expr[:100] + "..." if len(expr) > 100 else expr
        
        # Convert field references [Field] -> col("Field")
        expr = self._convert_field_references(expr)
        
        # Convert IF/ELSEIF/ELSE/ENDIF to when/otherwise
        expr = self._convert_if_statements(expr)
        
        # Convert operators
        expr = self._convert_operators(expr)
        
        # Convert functions
        expr = self._convert_functions(expr)
        
        # Convert IN clauses
        expr = self._convert_in_clause(expr)
        
        # Convert LIKE
        expr = self._convert_like(expr)
        
        # Clean up
        expr = self._cleanup_expression(expr)
        
        return expr
    
    def _convert_field_references(self, expr: str) -> str:
        """Convert [FieldName] to col("FieldName")"""
        # Handle nested brackets carefully
        result = []
        i = 0
        while i < len(expr):
            if expr[i] == '[':
                # Find matching ]
                j = i + 1
                while j < len(expr) and expr[j] != ']':
                    j += 1
                field_name = expr[i+1:j]
                result.append(f'col("{field_name}")')
                i = j + 1
            else:
                result.append(expr[i])
                i += 1
        return ''.join(result)
    
    def _convert_if_statements(self, expr: str) -> str:
        """Convert IF/ELSEIF/ELSE/ENDIF to when/otherwise chain"""
        # Check if expression contains IF
        if not re.search(r'\bif\b', expr, re.IGNORECASE):
            return expr
        
        # Pattern for full IF statement
        # This is complex, so we'll do a simpler approach for common patterns
        
        # Simple IF THEN ELSE ENDIF
        simple_if = re.compile(
            r'\bif\s+(.+?)\s+then\s+(.+?)\s+else\s+(.+?)\s+endif\b',
            re.IGNORECASE | re.DOTALL
        )
        
        def replace_simple_if(match):
            condition = match.group(1).strip()
            then_val = match.group(2).strip()
            else_val = match.group(3).strip()
            return f'when({condition}, {then_val}).otherwise({else_val})'
        
        expr = simple_if.sub(replace_simple_if, expr)
        
        # Handle elseif chains (more complex)
        # Pattern: if cond1 then val1 elseif cond2 then val2 ... else valN endif
        elseif_pattern = re.compile(
            r'\bif\s+(.+?)\s+then\s+(.+?)(?:\s+elseif\s+(.+?)\s+then\s+(.+?))*\s+else\s+(.+?)\s+endif\b',
            re.IGNORECASE | re.DOTALL
        )
        
        # For complex elseif, we need iterative processing
        while 'elseif' in expr.lower():
            # Find and convert one elseif at a time
            match = re.search(
                r'\bif\s+(.+?)\s+then\s+(.+?)\s+elseif\s+',
                expr, re.IGNORECASE | re.DOTALL
            )
            if match:
                # Complex chain - convert step by step
                expr = re.sub(
                    r'\belseif\b', '.when', expr, count=1, flags=re.IGNORECASE
                )
                expr = re.sub(
                    r'\bif\s+(.+?)\s+then\s+(.+?)\s*\.when',
                    r'when(\1, \2).when',
                    expr, count=1, flags=re.IGNORECASE | re.DOTALL
                )
            else:
                break
        
        # Final cleanup for then/else/endif
        expr = re.sub(r'\s+then\s+', ', ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\s+else\s+', ').otherwise(', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\s+endif\b', ')', expr, flags=re.IGNORECASE)
        
        return expr
    
    def _convert_operators(self, expr: str) -> str:
        """Convert Alteryx operators to PySpark"""
        # Logical operators
        expr = re.sub(r'\bAND\b', ' & ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bOR\b', ' | ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bNOT\s+', ' ~', expr, flags=re.IGNORECASE)
        
        # Comparison operators
        expr = re.sub(r'<>', ' != ', expr)
        expr = re.sub(r'(?<!=)=(?!=)', ' == ', expr)  # = but not == or !=
        
        # Handle != that might have been converted wrongly
        expr = re.sub(r'!\s*=\s*=', '!=', expr)
        
        return expr
    
    def _convert_functions(self, expr: str) -> str:
        """Convert Alteryx functions to PySpark"""
        for alteryx_func, pyspark_func in self.function_map.items():
            # Pattern to match function calls
            pattern = re.compile(
                rf'\b{alteryx_func}\s*\(', 
                re.IGNORECASE
            )
            
            if pattern.search(expr):
                if '{' in pyspark_func:
                    # Complex replacement with argument reordering
                    expr = self._replace_function_with_template(
                        expr, alteryx_func, pyspark_func
                    )
                else:
                    # Simple function name replacement
                    expr = pattern.sub(f'{pyspark_func}(', expr)
        
        # Handle Null() specifically
        expr = re.sub(r'\bNull\(\)', 'lit(None)', expr, flags=re.IGNORECASE)
        
        return expr
    
    def _replace_function_with_template(self, expr: str, func_name: str, template: str) -> str:
        """Replace function with template that may reorder arguments"""
        pattern = re.compile(
            rf'\b{func_name}\s*\(([^)]*)\)',
            re.IGNORECASE
        )
        
        def replacer(match):
            args_str = match.group(1)
            args = self._split_args(args_str)
            try:
                return template.format(*args)
            except (IndexError, KeyError):
                return match.group(0)  # Return original if template fails
        
        return pattern.sub(replacer, expr)
    
    def _split_args(self, args_str: str) -> List[str]:
        """Split function arguments, respecting nested parentheses"""
        args = []
        current = []
        depth = 0
        
        for char in args_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                args.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            args.append(''.join(current).strip())
        
        return args
    
    def _convert_in_clause(self, expr: str) -> str:
        """Convert IN ('a', 'b', 'c') to .isin(['a', 'b', 'c'])"""
        pattern = re.compile(
            r'(col\([^)]+\))\s+[Ii][Nn]\s*\(([^)]+)\)'
        )
        
        def replacer(match):
            column = match.group(1)
            values = match.group(2)
            return f'{column}.isin([{values}])'
        
        return pattern.sub(replacer, expr)
    
    def _convert_like(self, expr: str) -> str:
        """Convert LIKE to .like()"""
        pattern = re.compile(
            r'(col\([^)]+\))\s+[Ll][Ii][Kk][Ee]\s+([\'"][^"\']+[\'"])'
        )
        
        def replacer(match):
            column = match.group(1)
            pattern_str = match.group(2)
            return f'{column}.like({pattern_str})'
        
        return pattern.sub(replacer, expr)
    
    def _cleanup_expression(self, expr: str) -> str:
        """Final cleanup of expression"""
        # Remove extra whitespace
        expr = re.sub(r'\s+', ' ', expr).strip()
        
        # Fix double operators
        expr = re.sub(r'& &', '&', expr)
        expr = re.sub(r'\| \|', '|', expr)
        
        return expr

# COMMAND ----------
# MAGIC %md
# MAGIC ## Enhanced XML Parser

# COMMAND ----------
class AlteryxWorkflowParser:
    """Parses Alteryx .yxmd XML files with full configuration extraction"""
    
    TOOL_TYPE_MAP = {
        "DbFileInput": "Input Data",
        "AlteryxSelect": "Select",
        "Filter": "Filter",
        "Formula": "Formula",
        "Join": "Join",
        "Union": "Union",
        "Summarize": "Summarize",
        "Sort": "Sort",
        "Sample": "Sample",
        "Unique": "Unique",
        "CrossTab": "Cross Tab",
        "Transpose": "Transpose",
        "TextInput": "Text Input",
        "TextToColumns": "Text to Columns",
        "RegEx": "RegEx",
        "BrowseV2": "Browse",
        "AlteryxDbFileOutput": "Output Data",
        "DbFileOutput": "Output Data",
        "LockInFilter": "In-DB Filter",
        "LockInSelect": "In-DB Select",
        "LockInJoin": "In-DB Join",
        "LockInStreamIn": "In-DB Connect",
        "LockInStreamOut": "In-DB Stream Out",
        "FindReplace": "Find Replace",
        "MultiRowFormula": "Multi-Row Formula",
        "MultiFieldFormula": "Multi-Field Formula",
        "RecordID": "Record ID",
        "RunningTotal": "Running Total",
        "DateTime": "DateTime",
        "GenerateRows": "Generate Rows",
        "ToolContainer": "Container",
        "TextBox": "Comment",
        "AppendFields": "Append Fields",
        "AutoField": "Auto Field",
        "Imputation": "Imputation",
        "SampleN": "Random Sample",
    }
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.tree = None
        self.root = None
        self.expression_converter = AlteryxExpressionConverter()
        
    def parse(self) -> AlteryxWorkflow:
        """Parse the workflow file"""
        self.tree = ET.parse(self.file_path)
        self.root = self.tree.getroot()
        
        workflow_name = self._get_workflow_name()
        version = self.root.get("yxmdVer", "Unknown")
        constants = self._parse_constants()
        containers, standalone_tools = self._parse_nodes()
        connections = self._parse_connections()
        
        return AlteryxWorkflow(
            name=workflow_name,
            version=version,
            containers=containers,
            tools=standalone_tools,
            connections=connections,
            constants=constants
        )
    
    def _get_workflow_name(self) -> str:
        """Extract workflow name"""
        meta_info = self.root.find(".//MetaInfo")
        if meta_info is not None:
            name_elem = meta_info.find("n")
            if name_elem is not None and name_elem.text:
                return name_elem.text
        return "Unknown Workflow"
    
    def _parse_constants(self) -> Dict[str, Any]:
        """Parse user constants"""
        constants = {}
        constants_elem = self.root.find(".//Constants")
        if constants_elem is not None:
            for const in constants_elem.findall("Constant"):
                namespace = const.findtext("Namespace", "")
                name = const.findtext("n", "")
                value = const.findtext("Value", "")
                is_numeric = const.find("IsNumeric")
                is_num = is_numeric.get("value", "False") == "True" if is_numeric is not None else False
                
                key = f"{namespace}.{name}" if namespace else name
                constants[key] = {"value": value, "is_numeric": is_num}
        return constants
    
    def _parse_nodes(self):
        """Parse all nodes"""
        containers = []
        standalone_tools = []
        container_tool_ids = set()
        
        nodes_elem = self.root.find("Nodes")
        if nodes_elem is None:
            return containers, standalone_tools
        
        # First pass: identify containers
        for node in nodes_elem.findall("Node"):
            gui_settings = node.find("GuiSettings")
            if gui_settings is not None:
                plugin = gui_settings.get("Plugin", "")
                if "ToolContainer" in plugin:
                    container = self._parse_container(node)
                    if container:
                        containers.append(container)
                        container_tool_ids.update(self._get_container_tool_ids(node))
        
        # Second pass: standalone tools
        for node in nodes_elem.findall("Node"):
            tool_id = node.get("ToolID", "")
            if tool_id not in container_tool_ids:
                gui_settings = node.find("GuiSettings")
                if gui_settings is not None:
                    plugin = gui_settings.get("Plugin", "")
                    if "ToolContainer" not in plugin and "TextBox" not in plugin:
                        tool = self._parse_tool(node)
                        if tool:
                            standalone_tools.append(tool)
        
        return containers, standalone_tools
    
    def _get_container_tool_ids(self, container_node) -> set:
        """Get all tool IDs within a container"""
        tool_ids = {container_node.get("ToolID", "")}
        child_nodes = container_node.find(".//ChildNodes")
        if child_nodes is not None:
            for child in child_nodes.findall("Node"):
                tool_ids.add(child.get("ToolID", ""))
                tool_ids.update(self._get_container_tool_ids(child))
        return tool_ids
    
    def _parse_container(self, node) -> Optional[AlteryxContainer]:
        """Parse container node"""
        tool_id = node.get("ToolID", "")
        
        props = node.find("Properties/Configuration")
        caption = f"Container_{tool_id}"
        disabled = False
        if props is not None:
            caption = props.findtext("Caption", caption)
            disabled_elem = props.find("Disabled")
            disabled = disabled_elem.get("value", "False") == "True" if disabled_elem is not None else False
        
        gui_settings = node.find("GuiSettings")
        position = self._parse_position(gui_settings)
        
        tools = []
        child_containers = []
        child_nodes = node.find(".//ChildNodes")
        if child_nodes is not None:
            for child in child_nodes.findall("Node"):
                child_gui = child.find("GuiSettings")
                if child_gui is not None:
                    child_plugin = child_gui.get("Plugin", "")
                    if "ToolContainer" in child_plugin:
                        child_container = self._parse_container(child)
                        if child_container:
                            child_containers.append(child_container)
                    elif "TextBox" not in child_plugin:
                        tool = self._parse_tool(child)
                        if tool:
                            tools.append(tool)
        
        return AlteryxContainer(
            tool_id=tool_id,
            caption=caption,
            position=position,
            tools=tools,
            child_containers=child_containers,
            disabled=disabled
        )
    
    def _parse_tool(self, node) -> Optional[AlteryxTool]:
        """Parse tool node with full configuration"""
        tool_id = node.get("ToolID", "")
        
        gui_settings = node.find("GuiSettings")
        if gui_settings is None:
            return None
            
        plugin = gui_settings.get("Plugin", "")
        if not plugin:
            engine_settings = node.find(".//EngineSettings")
            if engine_settings is not None:
                macro = engine_settings.get("Macro", "")
                if macro:
                    plugin = f"Macro:{macro}"
        
        tool_type = self._get_tool_type(plugin)
        position = self._parse_position(gui_settings)
        annotation = self._parse_annotation(node)
        
        # Create base tool
        tool = AlteryxTool(
            tool_id=tool_id,
            plugin=plugin,
            tool_type=tool_type,
            position=position,
            annotation=annotation
        )
        
        # Parse tool-specific configuration
        config_elem = node.find("Properties/Configuration")
        if config_elem is not None:
            self._parse_tool_config(tool, config_elem)
        
        return tool
    
    def _parse_tool_config(self, tool: AlteryxTool, config_elem):
        """Parse tool-specific configuration"""
        if tool.tool_type == "Input Data":
            tool.input_config = self._parse_input_config(config_elem)
        elif tool.tool_type == "Select" or tool.tool_type == "In-DB Select":
            tool.select_fields = self._parse_select_config(config_elem)
        elif tool.tool_type == "Filter" or tool.tool_type == "In-DB Filter":
            tool.filter_expression, tool.filter_mode = self._parse_filter_config(config_elem)
        elif tool.tool_type == "Formula":
            tool.formula_fields = self._parse_formula_config(config_elem)
        elif tool.tool_type == "Join" or tool.tool_type == "In-DB Join":
            tool.join_info = self._parse_join_config(config_elem)
        elif tool.tool_type == "Summarize":
            tool.summarize_fields = self._parse_summarize_config(config_elem)
        elif tool.tool_type == "Sort":
            tool.sort_fields = self._parse_sort_config(config_elem)
        elif tool.tool_type == "Text Input":
            tool.text_input_data = self._parse_text_input_config(config_elem)
        elif tool.tool_type == "Text to Columns":
            tool.text_to_columns_config = self._parse_text_to_columns_config(config_elem)
        elif tool.tool_type == "Cross Tab":
            tool.crosstab_config = self._parse_crosstab_config(config_elem)
        elif tool.tool_type == "Unique":
            tool.unique_fields = self._parse_unique_config(config_elem)
        elif tool.tool_type == "Sample":
            tool.sample_config = self._parse_sample_config(config_elem)
        elif tool.tool_type == "Output Data":
            tool.output_config = self._parse_output_config(config_elem)
        elif tool.tool_type == "Find Replace":
            tool.find_replace_config = self._parse_find_replace_config(config_elem)
        
        # Store raw config for reference
        tool.raw_config = self._element_to_dict(config_elem)
    
    def _parse_input_config(self, config_elem) -> Dict:
        """Parse Input Data configuration"""
        config = {}
        
        file_elem = config_elem.find("File")
        if file_elem is not None:
            config['file_path'] = file_elem.text or ""
            config['format'] = file_elem.get("FileFormat", "")
            config['record_limit'] = file_elem.get("RecordLimit", "")
        
        # For database connections
        passwords = config_elem.findtext("Passwords", "")
        if passwords:
            config['has_password'] = True
        
        return config
    
    def _parse_select_config(self, config_elem) -> List[SelectField]:
        """Parse Select tool configuration"""
        fields = []
        
        select_fields = config_elem.find("SelectFields")
        if select_fields is not None:
            for sf in select_fields.findall("SelectField"):
                field_name = sf.get("field", "")
                selected = sf.get("selected", "True") == "True"
                rename = sf.get("rename")
                field_type = sf.get("type")
                size = sf.get("size")
                
                fields.append(SelectField(
                    field_name=field_name,
                    selected=selected,
                    rename=rename if rename and rename != field_name else None,
                    field_type=field_type,
                    size=int(size) if size else None
                ))
        
        return fields
    
    def _parse_filter_config(self, config_elem) -> Tuple[str, str]:
        """Parse Filter configuration"""
        mode = config_elem.findtext("Mode", "Custom")
        expression = ""
        
        if mode == "Custom":
            expression = config_elem.findtext("Expression", "")
        else:
            # Simple mode
            simple = config_elem.find("Simple")
            if simple is not None:
                field = simple.findtext("Field", "")
                operator = simple.findtext("Operator", "")
                operand = simple.find("Operands/Operand")
                operand_val = operand.text if operand is not None and operand.text else ""
                
                # Build expression
                if operator == "IsNull":
                    expression = f'[{field}] IS NULL'
                elif operator == "IsNotNull":
                    expression = f'[{field}] IS NOT NULL'
                elif operator == "=":
                    expression = f'[{field}] = "{operand_val}"'
                elif operator in ["<", ">", "<=", ">=", "!="]:
                    expression = f'[{field}] {operator} "{operand_val}"'
                elif operator == "Contains":
                    expression = f'Contains([{field}], "{operand_val}")'
        
        return expression, mode
    
    def _parse_formula_config(self, config_elem) -> List[FormulaField]:
        """Parse Formula configuration"""
        fields = []
        
        formula_fields = config_elem.find("FormulaFields")
        if formula_fields is not None:
            for ff in formula_fields.findall("FormulaField"):
                field_name = ff.get("field", "")
                expression = ff.get("expression", "")
                field_type = ff.get("type", "V_WString")
                size = ff.get("size")
                
                fields.append(FormulaField(
                    field_name=field_name,
                    expression=expression,
                    field_type=field_type,
                    size=int(size) if size else None
                ))
        
        return fields
    
    def _parse_join_config(self, config_elem) -> JoinInfo:
        """Parse Join configuration"""
        left_keys = []
        right_keys = []
        
        for join_info in config_elem.findall("JoinInfo"):
            connection = join_info.get("connection", "")
            for field in join_info.findall("Field"):
                field_name = field.get("field", "")
                if connection == "Left":
                    left_keys.append(field_name)
                elif connection == "Right":
                    right_keys.append(field_name)
        
        # Parse select configuration for join output
        select_config = {}
        select_conf = config_elem.find("SelectConfiguration/Configuration")
        if select_conf is not None:
            select_fields = []
            for sf in select_conf.findall(".//SelectField"):
                field_name = sf.get("field", "")
                selected = sf.get("selected", "True") == "True"
                rename = sf.get("rename")
                input_side = sf.get("input", "")
                
                select_fields.append({
                    'field': field_name,
                    'selected': selected,
                    'rename': rename,
                    'input': input_side
                })
            select_config['fields'] = select_fields
        
        return JoinInfo(
            left_keys=left_keys,
            right_keys=right_keys,
            select_config=select_config
        )
    
    def _parse_summarize_config(self, config_elem) -> List[SummarizeField]:
        """Parse Summarize configuration"""
        fields = []
        
        summarize_fields = config_elem.find("SummarizeFields")
        if summarize_fields is not None:
            for sf in summarize_fields.findall("SummarizeField"):
                field_name = sf.get("field", "")
                action = sf.get("action", "GroupBy")
                rename = sf.get("rename")
                
                fields.append(SummarizeField(
                    field_name=field_name,
                    action=action,
                    rename=rename if rename else field_name
                ))
        
        return fields
    
    def _parse_sort_config(self, config_elem) -> List[Tuple[str, str]]:
        """Parse Sort configuration"""
        fields = []
        
        sort_info = config_elem.find("SortInfo")
        if sort_info is not None:
            for field in sort_info.findall("Field"):
                field_name = field.get("field", "")
                order = field.get("order", "Ascending")
                fields.append((field_name, order))
        
        return fields
    
    def _parse_text_input_config(self, config_elem) -> TextInputData:
        """Parse Text Input configuration"""
        columns = []
        rows = []
        
        # Get field names
        fields_elem = config_elem.find("Fields")
        if fields_elem is not None:
            for field in fields_elem.findall("Field"):
                columns.append(field.get("name", ""))
        
        # Get data rows
        data_elem = config_elem.find("Data")
        if data_elem is not None:
            for row in data_elem.findall("r"):
                row_data = []
                for cell in row.findall("c"):
                    row_data.append(cell.text if cell.text else "")
                rows.append(row_data)
        
        return TextInputData(columns=columns, rows=rows)
    
    def _parse_text_to_columns_config(self, config_elem) -> Dict:
        """Parse Text to Columns configuration"""
        return {
            'field': config_elem.findtext("Field", ""),
            'delimiters': config_elem.find("Delimeters").get("value", ",") if config_elem.find("Delimeters") is not None else ",",
            'num_fields': config_elem.find("NumFields").get("value", "2") if config_elem.find("NumFields") is not None else "2",
            'root_name': config_elem.findtext("RootName", ""),
        }
    
    def _parse_crosstab_config(self, config_elem) -> Dict:
        """Parse Cross Tab configuration"""
        config = {
            'group_fields': [],
            'header_field': '',
            'data_field': '',
            'method': ''
        }
        
        for field in config_elem.findall(".//GroupFields/Field"):
            config['group_fields'].append(field.get("field", ""))
        
        config['header_field'] = config_elem.findtext(".//HeaderField", "")
        config['data_field'] = config_elem.findtext(".//DataField", "")
        config['method'] = config_elem.findtext(".//Method", "")
        
        return config
    
    def _parse_unique_config(self, config_elem) -> List[str]:
        """Parse Unique configuration"""
        fields = []
        unique_fields = config_elem.find("UniqueFields")
        if unique_fields is not None:
            for field in unique_fields.findall("Field"):
                fields.append(field.get("field", ""))
        return fields
    
    def _parse_sample_config(self, config_elem) -> Dict:
        """Parse Sample configuration"""
        return {
            'mode': config_elem.findtext("Mode", ""),
            'n': config_elem.findtext("N", "1000"),
            'group_fields': [f.get("field", "") for f in config_elem.findall(".//GroupFields/Field")]
        }
    
    def _parse_output_config(self, config_elem) -> Dict:
        """Parse Output configuration"""
        config = {}
        file_elem = config_elem.find("File")
        if file_elem is not None:
            config['file_path'] = file_elem.text or ""
            config['format'] = file_elem.get("FileFormat", "")
        
        config['output_mode'] = config_elem.findtext("Options/OutputMode", "Overwrite")
        
        return config
    
    def _parse_find_replace_config(self, config_elem) -> Dict:
        """Parse Find Replace configuration"""
        return {
            'field_find': config_elem.findtext("FieldFind", ""),
            'field_search': config_elem.findtext("FieldSearch", ""),
            'replace_field': config_elem.findtext("ReplaceFoundField", ""),
            'find_mode': config_elem.findtext("FindMode", ""),
            'append_fields': [f.get("field", "") for f in config_elem.findall(".//ReplaceAppendFields/Field")]
        }
    
    def _get_tool_type(self, plugin: str) -> str:
        """Map plugin to tool type"""
        for key, value in self.TOOL_TYPE_MAP.items():
            if key in plugin:
                return value
        if plugin.startswith("Macro:"):
            macro_name = plugin.replace("Macro:", "").replace(".yxmc", "")
            return f"Macro: {macro_name}"
        return plugin.split(".")[-1] if "." in plugin else plugin
    
    def _parse_position(self, gui_settings) -> Dict[str, int]:
        """Parse position"""
        if gui_settings is None:
            return {"x": 0, "y": 0}
        position = gui_settings.find("Position")
        if position is not None:
            return {
                "x": int(position.get("x", 0)),
                "y": int(position.get("y", 0)),
            }
        return {"x": 0, "y": 0}
    
    def _parse_annotation(self, node) -> str:
        """Parse annotation"""
        annotation = node.find("Properties/Annotation")
        if annotation is not None:
            text = annotation.findtext("AnnotationText", "")
            if not text:
                text = annotation.findtext("DefaultAnnotationText", "")
            return text
        return ""
    
    def _element_to_dict(self, element) -> Dict:
        """Convert element to dict"""
        result = {}
        for child in element:
            if len(child) == 0:
                if child.text:
                    result[child.tag] = child.text
                elif child.attrib:
                    result[child.tag] = child.attrib
            else:
                result[child.tag] = self._element_to_dict(child)
        return result
    
    def _parse_connections(self) -> List[AlteryxConnection]:
        """Parse connections"""
        connections = []
        connections_elem = self.root.find("Connections")
        if connections_elem is not None:
            for conn in connections_elem.findall("Connection"):
                origin = conn.find("Origin")
                dest = conn.find("Destination")
                if origin is not None and dest is not None:
                    connections.append(AlteryxConnection(
                        origin_tool_id=origin.get("ToolID", ""),
                        origin_connection=origin.get("Connection", "Output"),
                        destination_tool_id=dest.get("ToolID", ""),
                        destination_connection=dest.get("Connection", "Input"),
                        name=conn.get("name", "")
                    ))
        return connections

# COMMAND ----------
# MAGIC %md
# MAGIC ## Enhanced Code Generator

# COMMAND ----------
class PySparkCodeGenerator:
    """Generates accurate PySpark code from parsed Alteryx workflow"""
    
    def __init__(self, workflow: AlteryxWorkflow):
        self.workflow = workflow
        self.generated_code = []
        self.tool_to_df_map = {}
        self.df_counter = 0
        self.expression_converter = AlteryxExpressionConverter()
        
        # Build connection map for input tracking
        self.input_connections = defaultdict(list)
        self.output_connections = defaultdict(list)
        for conn in workflow.connections:
            self.input_connections[conn.destination_tool_id].append(conn)
            self.output_connections[conn.origin_tool_id].append(conn)
    
    def generate(self) -> str:
        """Generate complete PySpark notebook"""
        self.generated_code = []
        
        self._add_header()
        self._add_imports()
        self._add_configuration()
        self._add_helper_functions()
        
        # Process containers in order
        for container in self.workflow.containers:
            if not container.disabled:
                self._process_container(container)
        
        # Process standalone tools
        if self.workflow.tools:
            self._add_section("Standalone Tools")
            for tool in self.workflow.tools:
                self._generate_tool_code(tool)
        
        self._add_footer()
        
        return "\n".join(self.generated_code)
    
    def _add_line(self, line: str = ""):
        self.generated_code.append(line)
    
    def _add_cell_separator(self):
        self._add_line("")
        self._add_line("# COMMAND ----------")
        self._add_line("")
    
    def _add_markdown_cell(self, content: str):
        self._add_cell_separator()
        self._add_line("# MAGIC %md")
        for line in content.split("\n"):
            self._add_line(f"# MAGIC {line}")
    
    def _add_section(self, title: str, description: str = ""):
        self._add_markdown_cell(f"## {title}\n{description}" if description else f"## {title}")
        self._add_cell_separator()
    
    def _add_header(self):
        self._add_line("# Databricks notebook source")
        self._add_markdown_cell(f"""# {self.workflow.name}
## Converted from Alteryx Workflow
**Alteryx Version:** {self.workflow.version}  
**Conversion Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Workflow Summary
- **Containers:** {len(self.workflow.containers)}
- **Standalone Tools:** {len(self.workflow.tools)}
- **Connections:** {len(self.workflow.connections)}
- **User Constants:** {len(self.workflow.constants)}""")
    
    def _add_imports(self):
        self._add_section("Setup and Imports")
        self._add_line("""from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, trim, upper, lower, initcap,
    coalesce, isnull, isnotnull, regexp_replace, regexp_extract, substring,
    split, explode, array, struct, to_date, to_timestamp, date_format,
    year, month, dayofmonth, hour, minute, second, current_timestamp, current_date,
    datediff, date_add, date_sub, months_between, add_months,
    sum as _sum, count, avg, min as _min, max as _max, first, last,
    collect_list, collect_set, size, array_distinct, array_union,
    row_number, rank, dense_rank, lead, lag, ntile,
    broadcast, expr, length, lpad, rpad, locate, instr,
    abs, ceil, floor, round, sqrt, pow, log, log10, exp,
    greatest, least, hash, md5, sha1, sha2,
    from_json, to_json, get_json_object, json_tuple,
    monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    DecimalType, ArrayType, MapType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)""")
    
    def _add_configuration(self):
        self._add_section("Configuration", "Extracted from Alteryx User Constants")
        
        config_lines = ["config = {"]
        for key, value in self.workflow.constants.items():
            val = value["value"]
            clean_key = key.replace(".", "_")
            if value["is_numeric"]:
                config_lines.append(f'    "{clean_key}": {val},')
            else:
                config_lines.append(f'    "{clean_key}": "{val}",')
        
        config_lines.extend([
            '    "input_path": "/mnt/data/input/",',
            '    "output_path": "/mnt/data/output/",',
            "}"
        ])
        
        self._add_line("\n".join(config_lines))
        self._add_line('\nlogger.info(f"Configuration: {config}")')
    
    def _add_helper_functions(self):
        self._add_section("Helper Functions")
        self._add_line('''def log_df(df, name):
    """Log DataFrame info"""
    cnt = df.count()
    logger.info(f"{name}: {cnt:,} rows, {len(df.columns)} columns")
    return df

def cleanse_columns(df, columns, upper_case=True, trim_spaces=True):
    """Apply cleansing (Alteryx Cleanse macro equivalent)"""
    for c in columns:
        if c in df.columns:
            transformed = col(c)
            if trim_spaces:
                transformed = trim(transformed)
            if upper_case:
                transformed = upper(transformed)
            df = df.withColumn(c, transformed)
    return df''')
    
    def _process_container(self, container: AlteryxContainer, level: int = 0):
        """Process container and tools"""
        prefix = "#" * min(level + 2, 4)
        
        # Container header
        tool_summary = ", ".join([f"{t.tool_type}(ID:{t.tool_id})" for t in container.tools[:5]])
        if len(container.tools) > 5:
            tool_summary += f" +{len(container.tools)-5} more"
        
        self._add_markdown_cell(f"""{prefix} Container: {container.caption}
**ID:** {container.tool_id} | **Tools:** {len(container.tools)}  
{tool_summary}""")
        
        # Process nested containers
        for child in container.child_containers:
            if not child.disabled:
                self._process_container(child, level + 1)
        
        # Process tools
        for tool in container.tools:
            self._generate_tool_code(tool, container.caption)
    
    def _get_df_name(self, tool_id: str) -> str:
        """Get or create DataFrame name for tool"""
        if tool_id not in self.tool_to_df_map:
            self.df_counter += 1
            self.tool_to_df_map[tool_id] = f"df_{self.df_counter}"
        return self.tool_to_df_map[tool_id]
    
    def _get_input_df(self, tool_id: str) -> str:
        """Get input DataFrame reference for a tool"""
        connections = self.input_connections.get(tool_id, [])
        if connections:
            # Return first input (primary)
            origin_id = connections[0].origin_tool_id
            if origin_id in self.tool_to_df_map:
                return self.tool_to_df_map[origin_id]
        return "df_input  # TODO: Set input DataFrame"
    
    def _get_all_inputs(self, tool_id: str) -> List[str]:
        """Get all input DataFrame references"""
        connections = self.input_connections.get(tool_id, [])
        inputs = []
        for conn in connections:
            if conn.origin_tool_id in self.tool_to_df_map:
                inputs.append(self.tool_to_df_map[conn.origin_tool_id])
        return inputs if inputs else ["df_input  # TODO: Set input"]
    
    def _generate_tool_code(self, tool: AlteryxTool, container_name: str = ""):
        """Generate code for a tool"""
        self._add_cell_separator()
        
        df_name = self._get_df_name(tool.tool_id)
        annotation = f" | {tool.annotation}" if tool.annotation else ""
        
        self._add_line(f"# === {tool.tool_type} (Tool ID: {tool.tool_id}){annotation} ===")
        if container_name:
            self._add_line(f"# Container: {container_name}")
        
        # Generate based on tool type
        if tool.tool_type == "Input Data":
            self._gen_input(tool, df_name)
        elif tool.tool_type in ["Select", "In-DB Select"]:
            self._gen_select(tool, df_name)
        elif tool.tool_type in ["Filter", "In-DB Filter"]:
            self._gen_filter(tool, df_name)
        elif tool.tool_type == "Formula":
            self._gen_formula(tool, df_name)
        elif tool.tool_type in ["Join", "In-DB Join"]:
            self._gen_join(tool, df_name)
        elif tool.tool_type == "Union":
            self._gen_union(tool, df_name)
        elif tool.tool_type == "Summarize":
            self._gen_summarize(tool, df_name)
        elif tool.tool_type == "Sort":
            self._gen_sort(tool, df_name)
        elif tool.tool_type == "Text Input":
            self._gen_text_input(tool, df_name)
        elif tool.tool_type == "Text to Columns":
            self._gen_text_to_columns(tool, df_name)
        elif tool.tool_type == "Cross Tab":
            self._gen_crosstab(tool, df_name)
        elif tool.tool_type == "Unique":
            self._gen_unique(tool, df_name)
        elif tool.tool_type == "Sample":
            self._gen_sample(tool, df_name)
        elif tool.tool_type == "Browse":
            self._gen_browse(tool, df_name)
        elif tool.tool_type == "Output Data":
            self._gen_output(tool, df_name)
        elif tool.tool_type == "Find Replace":
            self._gen_find_replace(tool, df_name)
        elif tool.tool_type == "In-DB Stream Out":
            self._gen_stream_out(tool, df_name)
        elif "Macro:" in tool.tool_type:
            self._gen_macro(tool, df_name)
        else:
            self._gen_generic(tool, df_name)
    
    def _gen_input(self, tool: AlteryxTool, df_name: str):
        """Generate Input Data code"""
        config = tool.input_config or {}
        file_path = config.get('file_path', '')
        
        # Parse connection string to extract table/query info
        table_info = self._parse_connection_string(file_path)
        
        self._add_line(f'''# Source: {file_path[:100]}...
try:
    {df_name} = (
        spark.read
        .format("delta")
        .table("{table_info.get('table', 'catalog.schema.table_name')}")
        # OR use: .load("{table_info.get('path', '/path/to/data')}")
    )
    log_df({df_name}, "{df_name}")
except Exception as e:
    logger.error(f"Failed to read: {{e}}")
    raise''')
    
    def _gen_select(self, tool: AlteryxTool, df_name: str):
        """Generate Select code with exact field mappings"""
        input_df = self._get_input_df(tool.tool_id)
        fields = tool.select_fields or []
        
        selected = [f for f in fields if f.selected and f.field_name != "*Unknown"]
        deselected = [f for f in fields if not f.selected and f.field_name != "*Unknown"]
        renamed = [f for f in selected if f.rename]
        
        self._add_line(f"# Select: {len(selected)} fields selected, {len(deselected)} dropped, {len(renamed)} renamed")
        
        if selected:
            select_exprs = []
            for f in selected:
                if f.rename:
                    select_exprs.append(f'col("{f.field_name}").alias("{f.rename}")')
                else:
                    select_exprs.append(f'col("{f.field_name}")')
            
            # Check for *Unknown (select all remaining)
            has_unknown = any(f.field_name == "*Unknown" and f.selected for f in fields)
            
            if has_unknown and deselected:
                # Select all except dropped
                drop_list = [f'"{f.field_name}"' for f in deselected]
                self._add_line(f'''{df_name} = (
    {input_df}
    .drop({", ".join(drop_list)})''')
                if renamed:
                    for f in renamed:
                        self._add_line(f'    .withColumnRenamed("{f.field_name}", "{f.rename}")')
                self._add_line(")")
            else:
                self._add_line(f'''{df_name} = (
    {input_df}
    .select(
        {(",chr(10)+"        ").join(select_exprs)}
    )
)''')
        else:
            self._add_line(f"{df_name} = {input_df}")
        
        self._add_line(f'log_df({df_name}, "{df_name}")')
    
    def _gen_filter(self, tool: AlteryxTool, df_name: str):
        """Generate Filter code with converted expression"""
        input_df = self._get_input_df(tool.tool_id)
        expr = tool.filter_expression or ""
        
        pyspark_expr = self.expression_converter.convert(expr)
        
        self._add_line(f'''# Alteryx Filter: {expr[:80]}...
# Converted to PySpark:
filter_condition = {pyspark_expr}

{df_name}_true = {input_df}.filter(filter_condition)
{df_name}_false = {input_df}.filter(~(filter_condition))

# Primary output (True branch)
{df_name} = {df_name}_true
log_df({df_name}, "{df_name}")''')
    
    def _gen_formula(self, tool: AlteryxTool, df_name: str):
        """Generate Formula code with converted expressions"""
        input_df = self._get_input_df(tool.tool_id)
        formulas = tool.formula_fields or []
        
        self._add_line(f"{df_name} = {input_df}")
        
        for f in formulas:
            pyspark_expr = self.expression_converter.convert(f.expression)
            
            self._add_line(f'''
# Formula: {f.field_name} = {f.expression[:60]}...
{df_name} = {df_name}.withColumn(
    "{f.field_name}",
    {pyspark_expr}
)''')
        
        self._add_line(f'\nlog_df({df_name}, "{df_name}")')
    
    def _gen_join(self, tool: AlteryxTool, df_name: str):
        """Generate Join code with exact keys"""
        inputs = self._get_all_inputs(tool.tool_id)
        join_info = tool.join_info
        
        left_df = inputs[0] if len(inputs) > 0 else "df_left"
        right_df = inputs[1] if len(inputs) > 1 else "df_right"
        
        left_keys = join_info.left_keys if join_info else ["key"]
        right_keys = join_info.right_keys if join_info else ["key"]
        
        # Build join condition
        if len(left_keys) == 1 and left_keys[0] == right_keys[0]:
            join_cond = f'"{left_keys[0]}"'
        else:
            conditions = [f'({left_df}["{lk}"] == {right_df}["{rk}"])' for lk, rk in zip(left_keys, right_keys)]
            join_cond = " & ".join(conditions)
        
        self._add_line(f'''# Join Keys - Left: {left_keys}, Right: {right_keys}
{df_name}_joined = (
    {left_df}
    .join(
        {right_df},
        on={join_cond},
        how="inner"
    )
)

# Left unmatched (L output)
{df_name}_left_only = {left_df}.join({right_df}, on={join_cond}, how="left_anti")

# Right unmatched (R output)
{df_name}_right_only = {right_df}.join({left_df}, on={join_cond}, how="left_anti")

# Primary output (J - joined)
{df_name} = {df_name}_joined
log_df({df_name}, "{df_name}")''')
        
        # Handle select configuration for deselecting duplicate keys
        if join_info and join_info.select_config.get('fields'):
            deselect = [f['field'] for f in join_info.select_config['fields'] 
                       if not f['selected'] and f['field'].startswith('Right_')]
            if deselect:
                self._add_line(f"\n# Drop duplicate join keys from right side")
                for field in deselect[:5]:  # Limit to first 5 for readability
                    clean_name = field.replace("Right_", "")
                    self._add_line(f'# {df_name} = {df_name}.drop("{clean_name}")')
    
    def _gen_union(self, tool: AlteryxTool, df_name: str):
        """Generate Union code"""
        inputs = self._get_all_inputs(tool.tool_id)
        
        if len(inputs) >= 2:
            self._add_line(f'''{df_name} = (
    {inputs[0]}
    .unionByName({inputs[1]}, allowMissingColumns=True)''')
            for inp in inputs[2:]:
                self._add_line(f"    .unionByName({inp}, allowMissingColumns=True)")
            self._add_line(")")
        else:
            self._add_line(f"{df_name} = {inputs[0]}  # Single input - no union needed")
        
        self._add_line(f'log_df({df_name}, "{df_name}")')
    
    def _gen_summarize(self, tool: AlteryxTool, df_name: str):
        """Generate Summarize code"""
        input_df = self._get_input_df(tool.tool_id)
        fields = tool.summarize_fields or []
        
        group_by = [f for f in fields if f.action == "GroupBy"]
        agg_fields = [f for f in fields if f.action != "GroupBy"]
        
        # Map Alteryx actions to PySpark
        action_map = {
            "Sum": "_sum",
            "Count": "count",
            "CountDistinct": "countDistinct",
            "Avg": "avg",
            "Min": "_min",
            "Max": "_max",
            "First": "first",
            "Last": "last",
            "Concat": "collect_list",
            "CountNonNull": "count",
            "StdDev": "stddev",
            "Variance": "variance",
        }
        
        group_cols = ", ".join([f'"{f.field_name}"' for f in group_by])
        
        agg_exprs = []
        for f in agg_fields:
            pyspark_func = action_map.get(f.action, "first")
            alias = f.rename or f"{f.action}_{f.field_name}"
            
            if f.action == "Concat":
                agg_exprs.append(f'concat_ws(",", collect_list("{f.field_name}")).alias("{alias}")')
            elif f.action == "CountDistinct":
                agg_exprs.append(f'countDistinct("{f.field_name}").alias("{alias}")')
            else:
                agg_exprs.append(f'{pyspark_func}("{f.field_name}").alias("{alias}")')
        
        self._add_line(f'''{df_name} = (
    {input_df}
    .groupBy({group_cols})
    .agg(
        {(",chr(10)+"        ").join(agg_exprs)}
    )
)
log_df({df_name}, "{df_name}")''')
    
    def _gen_sort(self, tool: AlteryxTool, df_name: str):
        """Generate Sort code"""
        input_df = self._get_input_df(tool.tool_id)
        sort_fields = tool.sort_fields or []
        
        sort_exprs = []
        for field, order in sort_fields:
            if order.lower() == "descending":
                sort_exprs.append(f'col("{field}").desc()')
            else:
                sort_exprs.append(f'col("{field}").asc()')
        
        self._add_line(f'''{df_name} = (
    {input_df}
    .orderBy({", ".join(sort_exprs)})
)
log_df({df_name}, "{df_name}")''')
    
    def _gen_text_input(self, tool: AlteryxTool, df_name: str):
        """Generate Text Input code with exact data"""
        data = tool.text_input_data
        
        if data and data.columns:
            # Build schema
            schema_fields = ", ".join([f'StructField("{c}", StringType(), True)' for c in data.columns])
            
            # Build data rows
            data_rows = []
            for row in data.rows:
                escaped_row = [f'"{v}"' if v else 'None' for v in row]
                data_rows.append(f"    ({', '.join(escaped_row)}),")
            
            self._add_line(f'''# Text Input Data - {len(data.rows)} rows
schema_{tool.tool_id} = StructType([{schema_fields}])

data_{tool.tool_id} = [
{chr(10).join(data_rows)}
]

{df_name} = spark.createDataFrame(data_{tool.tool_id}, schema_{tool.tool_id})
log_df({df_name}, "{df_name}")''')
        else:
            self._add_line(f"# Text Input - No data extracted")
            self._add_line(f"{df_name} = spark.createDataFrame([], StructType([]))")
    
    def _gen_text_to_columns(self, tool: AlteryxTool, df_name: str):
        """Generate Text to Columns code"""
        input_df = self._get_input_df(tool.tool_id)
        config = tool.text_to_columns_config or {}
        
        field = config.get('field', 'column')
        delimiter = config.get('delimiters', ',')
        num_fields = int(config.get('num_fields', 2))
        root_name = config.get('root_name', field)
        
        self._add_line(f'''# Split "{field}" by "{delimiter}" into {num_fields} columns
{df_name} = (
    {input_df}
    .withColumn("_split_temp", split(col("{field}"), "{delimiter}"))''')
        
        for i in range(num_fields):
            self._add_line(f'    .withColumn("{root_name}{i+1}", col("_split_temp").getItem({i}))')
        
        self._add_line(f'''    .drop("_split_temp")
)
log_df({df_name}, "{df_name}")''')
    
    def _gen_crosstab(self, tool: AlteryxTool, df_name: str):
        """Generate Cross Tab code"""
        input_df = self._get_input_df(tool.tool_id)
        config = tool.crosstab_config or {}
        
        group_fields = config.get('group_fields', [])
        header_field = config.get('header_field', 'header')
        data_field = config.get('data_field', 'value')
        
        group_cols = ", ".join([f'"{f}"' for f in group_fields])
        
        self._add_line(f'''{df_name} = (
    {input_df}
    .groupBy({group_cols})
    .pivot("{header_field}")
    .agg(first("{data_field}"))
)
log_df({df_name}, "{df_name}")''')
    
    def _gen_unique(self, tool: AlteryxTool, df_name: str):
        """Generate Unique code"""
        input_df = self._get_input_df(tool.tool_id)
        fields = tool.unique_fields or []
        
        if fields:
            key_cols = ", ".join([f'"{f}"' for f in fields])
            self._add_line(f'''{df_name}_unique = {input_df}.dropDuplicates([{key_cols}])

# Get duplicates
_window = Window.partitionBy({key_cols}).orderBy({key_cols})
{df_name}_duplicates = (
    {input_df}
    .withColumn("_rn", row_number().over(_window))
    .filter(col("_rn") > 1)
    .drop("_rn")
)

{df_name} = {df_name}_unique
log_df({df_name}, "{df_name}")''')
        else:
            self._add_line(f"{df_name} = {input_df}.distinct()")
    
    def _gen_sample(self, tool: AlteryxTool, df_name: str):
        """Generate Sample code"""
        input_df = self._get_input_df(tool.tool_id)
        config = tool.sample_config or {}
        n = config.get('n', '1000')
        
        self._add_line(f'''{df_name} = {input_df}.limit({n})
# OR for random sample: {input_df}.sample(fraction=0.1)
log_df({df_name}, "{df_name}")''')
    
    def _gen_browse(self, tool: AlteryxTool, df_name: str):
        """Generate Browse equivalent"""
        input_df = self._get_input_df(tool.tool_id)
        self._add_line(f'''# Browse - Display data
display({input_df})
print(f"Row count: {{{input_df}.count():,}}")''')
    
    def _gen_output(self, tool: AlteryxTool, df_name: str):
        """Generate Output code"""
        input_df = self._get_input_df(tool.tool_id)
        config = tool.output_config or {}
        file_path = config.get('file_path', '')
        
        self._add_line(f'''# Output: {file_path[:80]}...
output_path = config["output_path"] + "output_table"

(
    {input_df}
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(output_path)
    # OR: .saveAsTable("catalog.schema.table_name")
)
logger.info(f"Written to {{output_path}}")''')
    
    def _gen_find_replace(self, tool: AlteryxTool, df_name: str):
        """Generate Find Replace code"""
        input_df = self._get_input_df(tool.tool_id)
        config = tool.find_replace_config or {}
        
        field_find = config.get('field_find', '')
        field_search = config.get('field_search', '')
        append_fields = config.get('append_fields', [])
        
        self._add_line(f'''# Find Replace: Match "{field_find}" with "{field_search}"
# Append fields: {append_fields}
{df_name} = (
    {input_df}
    .join(
        df_lookup,  # TODO: Set lookup DataFrame
        {input_df}["{field_find}"] == df_lookup["{field_search}"],
        how="left"
    )
)
log_df({df_name}, "{df_name}")''')
    
    def _gen_stream_out(self, tool: AlteryxTool, df_name: str):
        """Generate Stream Out (materialize In-DB query)"""
        input_df = self._get_input_df(tool.tool_id)
        self._add_line(f'''# In-DB Stream Out - Materialize query results
{df_name} = {input_df}
log_df({df_name}, "{df_name}")''')
    
    def _gen_macro(self, tool: AlteryxTool, df_name: str):
        """Generate Macro placeholder"""
        input_df = self._get_input_df(tool.tool_id)
        macro_name = tool.tool_type.replace("Macro: ", "")
        
        self._add_line(f'''# Macro: {macro_name}
# Common macros:
#   Cleanse -> cleanse_columns(df, columns)
#   CReW macros -> Custom implementation needed

{df_name} = {input_df}  # TODO: Implement macro logic
log_df({df_name}, "{df_name}")''')
    
    def _gen_generic(self, tool: AlteryxTool, df_name: str):
        """Generate generic placeholder"""
        input_df = self._get_input_df(tool.tool_id)
        
        self._add_line(f'''# {tool.tool_type} - Manual conversion required
# Plugin: {tool.plugin}
# Raw config: {str(tool.raw_config)[:200]}...

{df_name} = {input_df}  # TODO: Implement
log_df({df_name}, "{df_name}")''')
    
    def _parse_connection_string(self, conn_str: str) -> Dict:
        """Parse Alteryx connection string"""
        result = {'table': '', 'path': ''}
        
        # Extract table from SQL query
        match = re.search(r'from\s+([\w\.]+)', conn_str, re.IGNORECASE)
        if match:
            result['table'] = match.group(1)
        
        # Extract file path
        match = re.search(r'[A-Za-z]:\\[^|]+', conn_str)
        if match:
            result['path'] = match.group(0)
        
        return result
    
    def _add_footer(self):
        self._add_section("Execution Summary")
        self._add_line('''print("=" * 60)
print("Conversion Complete!")
print("=" * 60)
print(f"Generated at: {datetime.now()}")
print("\\nReview TODO comments for:")
print("  - Input source paths/tables")
print("  - Join key validations")
print("  - Complex expression conversions")
print("=" * 60)''')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execute Converter

# COMMAND ----------
# Parse workflow
print("Parsing workflow...")
parser = AlteryxWorkflowParser(workflow_file_path)
workflow = parser.parse()

print(f"✅ Parsed: {workflow.name}")
print(f"   Version: {workflow.version}")
print(f"   Containers: {len(workflow.containers)}")
print(f"   Tools: {len(workflow.tools)}")
print(f"   Connections: {len(workflow.connections)}")
print(f"   Constants: {len(workflow.constants)}")

# COMMAND ----------
# Display structure
print("\n📦 WORKFLOW STRUCTURE")
print("=" * 60)

def show_container(c, indent=0):
    pfx = "  " * indent
    status = "❌" if c.disabled else "✅"
    print(f"{pfx}{status} {c.caption} (ID:{c.tool_id}, Tools:{len(c.tools)})")
    for t in c.tools[:3]:
        print(f"{pfx}   └─ {t.tool_type} (ID:{t.tool_id})")
    if len(c.tools) > 3:
        print(f"{pfx}   └─ ... +{len(c.tools)-3} more")
    for child in c.child_containers:
        show_container(child, indent+1)

for container in workflow.containers:
    show_container(container)
    print()

# COMMAND ----------
# Generate code
print("Generating PySpark code...")
generator = PySparkCodeGenerator(workflow)
generated_code = generator.generate()

lines = generated_code.split('\n')
print(f"✅ Generated {len(lines):,} lines of code")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generated PySpark Notebook Code
# MAGIC 
# MAGIC Copy the output below to a new Databricks notebook.

# COMMAND ----------
print(generated_code)

# COMMAND ----------
# Save to file
if generate_file:
    try:
        with open(output_notebook_path, 'w') as f:
            f.write(generated_code)
        print(f"✅ Saved to: {output_notebook_path}")
    except Exception as e:
        print(f"⚠️ Could not save: {e}")
        print("Copy the code from the cell above.")

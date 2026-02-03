# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx to PySpark Converter Tool
# MAGIC ## Dynamic Workflow Conversion Utility
# MAGIC
# MAGIC This notebook reads any Alteryx workflow file (.yxmd) and automatically converts it to PySpark code.
# MAGIC
# MAGIC **Usage:**
# MAGIC 1. Upload your .yxmd file to Databricks Volumes
# MAGIC 2. Set the `workflow_file_path` in the configuration cell
# MAGIC 3. Run all cells to generate the PySpark notebook code
# MAGIC
# MAGIC **Output:**
# MAGIC - Parsed workflow structure with containers and tools
# MAGIC - Generated PySpark code for each container/section
# MAGIC - Ready-to-use Databricks notebook code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# CONFIGURATION - Set your workflow file path here
# Example: "/Volumes/catalog/schema/volume_name/your_workflow.yxmd"

workflow_file_path = "/Volumes/mydatabricks/customer360/data_360/Alteryx_workflow /S_Insurance_Consumption_Layer.yxmd"


# Output options
output_notebook_path = "/Volumes/mydatabricks/customer360/data_360/Alteryx_result/converted_workflow.py"
generate_file = True  # Set to True to save the generated notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converter Engine

# COMMAND ----------

import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import re
from datetime import datetime

# COMMAND ----------

# Data Classes for Workflow Structure

@dataclass
class AlteryxTool:
    """Represents an Alteryx tool/node"""
    tool_id: str
    plugin: str
    tool_type: str
    position: Dict[str, int]
    configuration: Dict[str, Any]
    annotation: str = ""
    connections_in: List[str] = field(default_factory=list)
    connections_out: List[str] = field(default_factory=list)
    
@dataclass
class AlteryxContainer:
    """Represents an Alteryx container grouping tools"""
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
# MAGIC ### XML Parser

# COMMAND ----------

class AlteryxWorkflowParser:
    """Parses Alteryx .yxmd XML files into structured objects"""
    
    # Tool type mapping from plugin names
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
        "LockInFilter": "In-DB Filter",
        "LockInSelect": "In-DB Select",
        "LockInJoin": "In-DB Join",
        "LockInStreamIn": "In-DB Connect",
        "LockInStreamOut": "In-DB Stream Out",
        "FindReplace": "Find Replace",
        "MultiRowFormula": "Multi-Row Formula",
        "RecordID": "Record ID",
        "RunningTotal": "Running Total",
        "DateTime": "DateTime",
        "GenerateRows": "Generate Rows",
        "ToolContainer": "Container",
        "TextBox": "Comment",
    }
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.tree = None
        self.root = None
        
    def parse(self) -> AlteryxWorkflow:
        """Parse the workflow file and return structured workflow object"""
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
        """Extract workflow name from metadata"""
        meta_info = self.root.find(".//MetaInfo")
        if meta_info is not None:
            name_elem = meta_info.find("n")
            if name_elem is not None and name_elem.text:
                return name_elem.text
        return "Unknown Workflow"
    
    def _parse_constants(self) -> Dict[str, Any]:
        """Parse user-defined constants"""
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
        """Parse all nodes (tools and containers)"""
        containers = []
        standalone_tools = []
        container_tool_ids = set()
        
        nodes_elem = self.root.find("Nodes")
        if nodes_elem is None:
            return containers, standalone_tools
        
        # First pass: identify all containers and their children
        for node in nodes_elem.findall("Node"):
            gui_settings = node.find("GuiSettings")
            if gui_settings is not None:
                plugin = gui_settings.get("Plugin", "")
                if "ToolContainer" in plugin:
                    container = self._parse_container(node)
                    if container:
                        containers.append(container)
                        # Track all tools inside containers
                        container_tool_ids.update(self._get_container_tool_ids(node))
        
        # Second pass: get standalone tools (not in containers)
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
        """Get all tool IDs within a container recursively"""
        tool_ids = {container_node.get("ToolID", "")}
        child_nodes = container_node.find(".//ChildNodes")
        if child_nodes is not None:
            for child in child_nodes.findall("Node"):
                tool_ids.add(child.get("ToolID", ""))
                # Recursively get nested container tools
                tool_ids.update(self._get_container_tool_ids(child))
        return tool_ids
    
    def _parse_container(self, node) -> Optional[AlteryxContainer]:
        """Parse a container node"""
        tool_id = node.get("ToolID", "")
        
        # Get container properties
        props = node.find("Properties/Configuration")
        caption = ""
        disabled = False
        if props is not None:
            caption = props.findtext("Caption", f"Container_{tool_id}")
            disabled_elem = props.find("Disabled")
            disabled = disabled_elem.get("value", "False") == "True" if disabled_elem is not None else False
        
        # Get position
        gui_settings = node.find("GuiSettings")
        position = self._parse_position(gui_settings)
        
        # Parse child tools
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
        """Parse a tool node"""
        tool_id = node.get("ToolID", "")
        
        gui_settings = node.find("GuiSettings")
        if gui_settings is None:
            return None
            
        plugin = gui_settings.get("Plugin", "")
        if not plugin:
            # Check for macro
            engine_settings = node.find(".//EngineSettings")
            if engine_settings is not None:
                macro = engine_settings.get("Macro", "")
                if macro:
                    plugin = f"Macro:{macro}"
        
        tool_type = self._get_tool_type(plugin)
        position = self._parse_position(gui_settings)
        configuration = self._parse_configuration(node)
        annotation = self._parse_annotation(node)
        
        return AlteryxTool(
            tool_id=tool_id,
            plugin=plugin,
            tool_type=tool_type,
            position=position,
            configuration=configuration,
            annotation=annotation
        )
    
    def _get_tool_type(self, plugin: str) -> str:
        """Map plugin name to tool type"""
        for key, value in self.TOOL_TYPE_MAP.items():
            if key in plugin:
                return value
        # Handle macros
        if plugin.startswith("Macro:"):
            macro_name = plugin.replace("Macro:", "").replace(".yxmc", "")
            return f"Macro: {macro_name}"
        return plugin.split(".")[-1] if "." in plugin else plugin
    
    def _parse_position(self, gui_settings) -> Dict[str, int]:
        """Parse position from GUI settings"""
        if gui_settings is None:
            return {"x": 0, "y": 0}
        position = gui_settings.find("Position")
        if position is not None:
            return {
                "x": int(position.get("x", 0)),
                "y": int(position.get("y", 0)),
                "width": int(position.get("width", 0)),
                "height": int(position.get("height", 0))
            }
        return {"x": 0, "y": 0}
    
    def _parse_configuration(self, node) -> Dict[str, Any]:
        """Parse tool configuration"""
        config = {}
        config_elem = node.find("Properties/Configuration")
        if config_elem is not None:
            config = self._element_to_dict(config_elem)
        return config
    
    def _parse_annotation(self, node) -> str:
        """Parse tool annotation/comment"""
        annotation = node.find("Properties/Annotation")
        if annotation is not None:
            text = annotation.findtext("AnnotationText", "")
            if not text:
                text = annotation.findtext("DefaultAnnotationText", "")
            return text
        return ""
    
    def _element_to_dict(self, element) -> Dict[str, Any]:
        """Convert XML element to dictionary"""
        result = {}
        for child in element:
            if len(child) == 0:
                # Leaf node
                if child.text:
                    result[child.tag] = child.text
                elif child.attrib:
                    result[child.tag] = child.attrib
            else:
                # Has children
                result[child.tag] = self._element_to_dict(child)
        return result
    
    def _parse_connections(self) -> List[AlteryxConnection]:
        """Parse all connections between tools"""
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
# MAGIC ### PySpark Code Generator

# COMMAND ----------

class PySparkCodeGenerator:
    """Generates PySpark code from parsed Alteryx workflow"""
    
    def __init__(self, workflow: AlteryxWorkflow):
        self.workflow = workflow
        self.generated_code = []
        self.tool_to_df_map = {}  # Maps tool_id to DataFrame variable name
        self.df_counter = 0
        
    def generate(self) -> str:
        """Generate complete PySpark notebook code"""
        self.generated_code = []
        
        # Add header
        self._add_header()
        
        # Add imports and setup
        self._add_imports()
        
        # Add configuration from constants
        self._add_configuration()
        
        # Add helper functions
        self._add_helper_functions()
        
        # Process containers
        for container in self.workflow.containers:
            if not container.disabled:
                self._process_container(container)
        
        # Process standalone tools
        if self.workflow.tools:
            self._add_section("Standalone Tools", "Tools not in any container")
            for tool in self.workflow.tools:
                self._generate_tool_code(tool)
        
        # Add footer
        self._add_footer()
        
        return "\n".join(self.generated_code)
    
    def _add_line(self, line: str = ""):
        """Add a line to generated code"""
        self.generated_code.append(line)
    
    def _add_cell_separator(self):
        """Add Databricks cell separator"""
        self._add_line("")
        self._add_line("# COMMAND ----------")
        self._add_line("")
    
    def _add_markdown_cell(self, content: str):
        """Add a markdown cell"""
        self._add_cell_separator()
        self._add_line("# MAGIC %md")
        for line in content.split("\n"):
            self._add_line(f"# MAGIC {line}")
    
    def _add_section(self, title: str, description: str = ""):
        """Add a section header"""
        self._add_markdown_cell(f"## {title}\n{description}" if description else f"## {title}")
        self._add_cell_separator()
    
    def _add_header(self):
        """Add notebook header"""
        self._add_line("# Databricks notebook source")
        self._add_markdown_cell(f"""# {self.workflow.name}
## Converted from Alteryx Workflow
**Alteryx Version:** {self.workflow.version}
**Conversion Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Workflow Structure:
- **Containers:** {len(self.workflow.containers)}
- **Standalone Tools:** {len(self.workflow.tools)}
- **Connections:** {len(self.workflow.connections)}
- **User Constants:** {len(self.workflow.constants)}""")
    
    def _add_imports(self):
        """Add import statements"""
        self._add_section("Setup and Imports")
        self._add_line("""# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, trim, upper, lower,
    coalesce, isnull, isnotnull, regexp_replace, substring,
    split, explode, array, struct, to_date, to_timestamp,
    year, month, dayofmonth, current_timestamp, current_date,
    sum as _sum, count, avg, min as _min, max as _max,
    first, last, collect_list, collect_set, size,
    row_number, rank, dense_rank, lead, lag,
    broadcast, expr, length, substring as str_substring
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    DecimalType, ArrayType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)""")
    
    def _add_configuration(self):
        """Add configuration from workflow constants"""
        self._add_section("Configuration", "Parameters extracted from Alteryx workflow constants")
        
        config_lines = ["# Configuration from Alteryx User Constants", "config = {"]
        
        for key, value in self.workflow.constants.items():
            val = value["value"]
            if value["is_numeric"]:
                config_lines.append(f'    "{key}": {val},')
            else:
                config_lines.append(f'    "{key}": "{val}",')
        
        # Add default paths
        config_lines.extend([
            '    "input_path": "/mnt/data/input/",',
            '    "output_path": "/mnt/data/output/",',
            '    "checkpoint_location": "/mnt/checkpoints/"',
            "}"
        ])
        
        self._add_line("\n".join(config_lines))
        self._add_line("")
        self._add_line('logger.info(f"Configuration loaded: {config}")')
    
    def _add_helper_functions(self):
        """Add reusable helper functions"""
        self._add_section("Helper Functions", "Reusable functions for common operations")
        self._add_line('''def log_dataframe_info(df, name, show_sample=False):
    """Log DataFrame information for debugging"""
    count = df.count()
    logger.info(f"DataFrame '{name}': {count:,} rows, {len(df.columns)} columns")
    if show_sample:
        df.show(5, truncate=False)
    return df

def apply_cleanse(df, columns, uppercase=True, trim_whitespace=True):
    """Apply cleansing operations (equivalent to Alteryx Cleanse macro)"""
    for col_name in columns:
        if col_name in df.columns:
            cleansed = col(col_name)
            if trim_whitespace:
                cleansed = trim(cleansed)
            if uppercase:
                cleansed = upper(cleansed)
            df = df.withColumn(col_name, cleansed)
    return df

def safe_join(df_left, df_right, left_keys, right_keys, join_type="inner", join_name=""):
    """Perform join with logging and null key validation"""
    # Validate keys
    for lk in left_keys if isinstance(left_keys, list) else [left_keys]:
        null_count = df_left.filter(col(lk).isNull()).count()
        if null_count > 0:
            logger.warning(f"Join '{join_name}': {null_count} null values in left key '{lk}'")
    
    # Perform join
    if isinstance(left_keys, list):
        join_condition = [df_left[lk] == df_right[rk] for lk, rk in zip(left_keys, right_keys)]
        result = df_left.join(df_right, join_condition, join_type)
    else:
        result = df_left.join(df_right, df_left[left_keys] == df_right[right_keys], join_type)
    
    logger.info(f"Join '{join_name}' completed: {result.count():,} rows")
    return result''')
    
    def _process_container(self, container: AlteryxContainer, level: int = 0):
        """Process a container and its contents"""
        prefix = "#" * (level + 2)
        
        # Add container section
        tool_list = ", ".join([f"{t.tool_type} (ID: {t.tool_id})" for t in container.tools[:5]])
        if len(container.tools) > 5:
            tool_list += f", ... (+{len(container.tools) - 5} more)"
        
        self._add_markdown_cell(f"""{prefix} Container: {container.caption}
**Container ID:** {container.tool_id}
**Tools Count:** {len(container.tools)}
**Tools:** {tool_list}""")
        
        # Process nested containers first
        for child_container in container.child_containers:
            if not child_container.disabled:
                self._process_container(child_container, level + 1)
        
        # Process tools in container
        for tool in container.tools:
            self._generate_tool_code(tool, container.caption)
    
    def _get_df_name(self, tool_id: str, prefix: str = "df") -> str:
        """Generate a DataFrame variable name for a tool"""
        if tool_id in self.tool_to_df_map:
            return self.tool_to_df_map[tool_id]
        
        self.df_counter += 1
        df_name = f"{prefix}_{self.df_counter}"
        self.tool_to_df_map[tool_id] = df_name
        return df_name
    
    def _generate_tool_code(self, tool: AlteryxTool, container_name: str = ""):
        """Generate PySpark code for a specific tool"""
        self._add_cell_separator()
        
        # Add tool comment
        annotation = f" - {tool.annotation}" if tool.annotation else ""
        self._add_line(f"# Alteryx Tool: {tool.tool_type} (ID: {tool.tool_id}){annotation}")
        if container_name:
            self._add_line(f"# Container: {container_name}")
        
        df_name = self._get_df_name(tool.tool_id)
        
        # Generate code based on tool type
        if tool.tool_type == "Input Data":
            self._generate_input_code(tool, df_name)
        elif tool.tool_type == "Select":
            self._generate_select_code(tool, df_name)
        elif tool.tool_type == "Filter":
            self._generate_filter_code(tool, df_name)
        elif tool.tool_type == "Formula":
            self._generate_formula_code(tool, df_name)
        elif tool.tool_type == "Join":
            self._generate_join_code(tool, df_name)
        elif tool.tool_type == "Union":
            self._generate_union_code(tool, df_name)
        elif tool.tool_type == "Summarize":
            self._generate_summarize_code(tool, df_name)
        elif tool.tool_type == "Sort":
            self._generate_sort_code(tool, df_name)
        elif tool.tool_type == "Text Input":
            self._generate_text_input_code(tool, df_name)
        elif tool.tool_type == "Text to Columns":
            self._generate_text_to_columns_code(tool, df_name)
        elif tool.tool_type == "Cross Tab":
            self._generate_crosstab_code(tool, df_name)
        elif tool.tool_type == "Unique":
            self._generate_unique_code(tool, df_name)
        elif tool.tool_type == "Sample":
            self._generate_sample_code(tool, df_name)
        elif tool.tool_type == "In-DB Filter":
            self._generate_indb_filter_code(tool, df_name)
        elif tool.tool_type == "In-DB Select":
            self._generate_indb_select_code(tool, df_name)
        elif tool.tool_type == "Browse":
            self._generate_browse_code(tool, df_name)
        elif tool.tool_type == "Output Data":
            self._generate_output_code(tool, df_name)
        elif tool.tool_type == "Find Replace":
            self._generate_find_replace_code(tool, df_name)
        elif "Macro:" in tool.tool_type:
            self._generate_macro_code(tool, df_name)
        else:
            self._generate_generic_code(tool, df_name)
    
    def _generate_input_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Input Data tool"""
        config = tool.configuration
        
        # Try to extract connection string and query
        file_config = config.get("File", "")
        
        self._add_line(f'''# TODO: Update the table/file path based on your Databricks environment
# Original Alteryx connection: {str(file_config)[:100]}...

try:
    {df_name} = (
        spark.read
        .format("delta")  # Change format as needed: csv, parquet, delta, jdbc
        .option("header", "true")
        .option("inferSchema", "true")
        # .option("url", "jdbc:...")  # For JDBC connections
        # .option("query", "SELECT * FROM ...")  # For JDBC queries
        .load("path/to/your/data")  # Update path
        # OR use: .table("catalog.schema.table_name")
    )
    log_dataframe_info({df_name}, "{df_name}")
except Exception as e:
    logger.error(f"Failed to read data: {{str(e)}}")
    raise''')
    
    def _generate_select_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Select tool"""
        config = tool.configuration
        select_fields = config.get("SelectFields", {})
        
        # Find input DataFrame
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Select/Rename columns
# TODO: Update input DataFrame reference and column selections

# Original configuration: {str(select_fields)[:200]}...

{df_name} = (
    {input_df}
    .select(
        # Add your column selections here:
        # col("original_name").alias("new_name"),
        # col("column_to_keep"),
        "*"  # Or select specific columns
    )
    # .drop("column_to_remove")  # Remove unwanted columns
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_filter_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Filter tool"""
        config = tool.configuration
        expression = config.get("Expression", "")
        mode = config.get("Mode", "Custom")
        
        input_df = self._find_input_df(tool.tool_id)
        
        # Try to convert Alteryx expression to PySpark
        pyspark_expr = self._convert_alteryx_expression(expression)
        
        self._add_line(f'''# Filter rows
# Alteryx Expression: {expression[:100] if expression else "N/A"}...

{df_name}_true = (
    {input_df}
    .filter({pyspark_expr})
)

{df_name}_false = (
    {input_df}
    .filter(~({pyspark_expr}))
)

# Use {df_name}_true for True output, {df_name}_false for False output
{df_name} = {df_name}_true
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_formula_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Formula tool"""
        config = tool.configuration
        formula_fields = config.get("FormulaFields", {})
        
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Formula - Create/Update columns
# Original configuration: {str(formula_fields)[:200]}...

{df_name} = (
    {input_df}
    .withColumn(
        "new_column_name",  # Update column name
        # Convert Alteryx formula to PySpark:
        # when(condition, value).otherwise(other_value)
        # concat(col("a"), lit(" "), col("b"))
        # col("column").cast("integer")
        lit(None)  # Placeholder - update with actual formula
    )
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_join_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Join tool"""
        config = tool.configuration
        
        self._add_line(f'''# Join DataFrames
# TODO: Update DataFrame references and join keys

# Original configuration: {str(config)[:200]}...

{df_name}_joined = (
    df_left  # Update with actual left DataFrame
    .join(
        df_right,  # Update with actual right DataFrame
        on="join_key",  # Update with actual join key(s)
        how="inner"  # inner, left, right, outer
    )
)

# Handle Left/Right/Join outputs
{df_name}_left_only = df_left.join(df_right, on="join_key", how="left_anti")
{df_name}_right_only = df_right.join(df_left, on="join_key", how="left_anti")
{df_name} = {df_name}_joined

log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_union_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Union tool"""
        self._add_line(f'''# Union DataFrames
# TODO: Update DataFrame references

{df_name} = (
    df_input_1  # Update with first input DataFrame
    .unionByName(
        df_input_2,  # Update with second input DataFrame
        allowMissingColumns=True
    )
    # Add more .unionByName() calls for additional inputs
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_summarize_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Summarize tool"""
        config = tool.configuration
        
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Summarize / Group By
# Original configuration: {str(config)[:200]}...

{df_name} = (
    {input_df}
    .groupBy(
        "group_column_1",  # Update with actual group columns
        # "group_column_2",
    )
    .agg(
        _sum("numeric_column").alias("sum_column"),
        count("*").alias("count"),
        avg("numeric_column").alias("avg_column"),
        _min("column").alias("min_column"),
        _max("column").alias("max_column"),
        first("column").alias("first_value"),
        collect_list("column").alias("list_values"),
        concat_ws(",", collect_list("column")).alias("concatenated")
    )
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_sort_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Sort tool"""
        config = tool.configuration
        sort_info = config.get("SortInfo", {})
        
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Sort DataFrame
# Original configuration: {str(sort_info)[:200]}...

{df_name} = (
    {input_df}
    .orderBy(
        col("sort_column_1").asc(),  # Update columns and order
        # col("sort_column_2").desc(),
    )
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_text_input_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Text Input tool"""
        config = tool.configuration
        
        # Extract data structure
        fields = config.get("Fields", {})
        data = config.get("Data", {})
        
        self._add_line(f'''# Text Input - Static Data
# Original fields: {str(fields)[:100]}...

# Define schema
schema = StructType([
    StructField("column1", StringType(), True),  # Update column names and types
    StructField("column2", StringType(), True),
])

# Define data
data = [
    ("value1", "value2"),  # Update with actual data rows
    ("value3", "value4"),
]

{df_name} = spark.createDataFrame(data, schema)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_text_to_columns_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Text to Columns tool"""
        config = tool.configuration
        field = config.get("Field", "")
        delimiters = config.get("Delimeters", {})
        num_fields = config.get("NumFields", {})
        
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Text to Columns - Split column by delimiter
# Original field: {field}, Delimiter: {delimiters}

{df_name} = (
    {input_df}
    .withColumn("_split_array", split(col("{field}"), "-"))  # Update delimiter
    .withColumn("{field}_1", col("_split_array").getItem(0))
    .withColumn("{field}_2", col("_split_array").getItem(1))
    # Add more columns as needed
    .drop("_split_array")
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_crosstab_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Cross Tab tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Cross Tab / Pivot
# TODO: Update group columns, pivot column, and aggregation

{df_name} = (
    {input_df}
    .groupBy("group_column")  # Update group column
    .pivot("pivot_column")  # Update pivot column
    .agg(first("value_column"))  # Update aggregation
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_unique_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Unique tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Unique - Remove duplicates
{df_name}_unique = (
    {input_df}
    .dropDuplicates(["key_column_1", "key_column_2"])  # Update key columns
)

# Get duplicates (if needed)
window_spec = Window.partitionBy("key_column_1", "key_column_2").orderBy("key_column_1")
{df_name}_duplicates = (
    {input_df}
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") > 1)
    .drop("_row_num")
)

{df_name} = {df_name}_unique
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_sample_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Sample tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Sample rows
{df_name} = (
    {input_df}
    .sample(fraction=0.1)  # Update sample fraction
    # OR .limit(1000)  # For fixed number of rows
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_indb_filter_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for In-DB Filter tool"""
        config = tool.configuration
        expression = config.get("Expression", "")
        
        input_df = self._find_input_df(tool.tool_id)
        pyspark_expr = self._convert_alteryx_expression(expression)
        
        self._add_line(f'''# In-DB Filter (converted to standard PySpark filter)
# Alteryx Expression: {expression[:150] if expression else "N/A"}...

{df_name} = (
    {input_df}
    .filter({pyspark_expr})
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_indb_select_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for In-DB Select tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# In-DB Select (converted to standard PySpark select)
# TODO: Update column selections based on original configuration

{df_name} = (
    {input_df}
    .select("*")  # Update with specific columns
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_browse_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Browse tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Browse - Display data (equivalent)
# In Databricks, you can use display() or show()

display({input_df})  # Interactive display in Databricks
# OR: {input_df}.show(20, truncate=False)
# OR: print(f"Row count: {{{input_df}.count():,}}")''')
    
    def _generate_output_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Output Data tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Output Data - Write to Delta table
# TODO: Update the output path/table name

output_path = config["output_path"] + "output_table"  # Update path

try:
    (
        {input_df}
        .write
        .format("delta")
        .mode("overwrite")  # overwrite, append, merge
        .option("overwriteSchema", "true")
        .partitionBy("partition_column")  # Optional: add partitioning
        .save(output_path)
        # OR: .saveAsTable("catalog.schema.table_name")
    )
    logger.info(f"Successfully wrote output to {{output_path}}")
except Exception as e:
    logger.error(f"Failed to write output: {{str(e)}}")
    raise''')
    
    def _generate_find_replace_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Find Replace tool"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Find Replace
# TODO: Update with actual find/replace logic

{df_name} = (
    {input_df}
    .join(
        df_lookup,  # Your lookup/mapping DataFrame
        on="lookup_key",
        how="left"
    )
    .withColumn(
        "target_column",
        coalesce(col("replacement_value"), col("original_value"))
    )
)
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_macro_code(self, tool: AlteryxTool, df_name: str):
        """Generate code for Alteryx Macro"""
        macro_name = tool.tool_type.replace("Macro: ", "")
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# Macro: {macro_name}
# TODO: Implement the macro logic or replace with equivalent PySpark code

# Common macros and their PySpark equivalents:
# - Cleanse: apply_cleanse(df, columns, uppercase=True, trim_whitespace=True)
# - Crew Macro: Custom logic needed
# - Date Time: date/time functions

{df_name} = {input_df}  # Update with actual macro logic
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _generate_generic_code(self, tool: AlteryxTool, df_name: str):
        """Generate generic code for unsupported tools"""
        input_df = self._find_input_df(tool.tool_id)
        
        self._add_line(f'''# {tool.tool_type} - Manual conversion needed
# Tool ID: {tool.tool_id}
# Plugin: {tool.plugin}
# Configuration: {str(tool.configuration)[:200]}...

# TODO: Implement PySpark equivalent for this tool

{df_name} = {input_df}  # Placeholder - update with actual logic
log_dataframe_info({df_name}, "{df_name}")''')
    
    def _find_input_df(self, tool_id: str) -> str:
        """Find the input DataFrame variable name for a tool"""
        for conn in self.workflow.connections:
            if conn.destination_tool_id == tool_id:
                if conn.origin_tool_id in self.tool_to_df_map:
                    return self.tool_to_df_map[conn.origin_tool_id]
        return "df_input  # TODO: Update with actual input DataFrame"
    
    def _convert_alteryx_expression(self, expression: str) -> str:
        """Convert Alteryx expression to PySpark expression"""
        if not expression:
            return 'lit(True)  # TODO: Add filter condition'
        
        # Basic conversions
        pyspark_expr = expression
        
        # Replace Alteryx syntax with PySpark
        replacements = [
            (r'\[([^\]]+)\]', r'col("\1")'),  # [Column] -> col("Column")
            (r'=(?!=)', r'=='),  # = -> == (but not ==)
            (r'<>', r'!='),  # <> -> !=
            (r'\bAND\b', r'&'),  # AND -> &
            (r'\bOR\b', r'|'),  # OR -> |
            (r'\bNOT\b', r'~'),  # NOT -> ~
            (r'\bNull\(\)', r'lit(None)'),  # Null() -> lit(None)
            (r'\bIsNull\(', r'isnull('),  # IsNull -> isnull
            (r'\bIsEmpty\(', r'(col("").isNull() | (col("") == ""))'),  # IsEmpty
            (r'\bContains\(([^,]+),\s*([^)]+)\)', r'\1.contains(\2)'),  # Contains
            (r'\bLIKE\b', r'.like'),  # LIKE
            (r'\bIN\s*\(', r'.isin('),  # IN (
        ]
        
        for pattern, replacement in replacements:
            pyspark_expr = re.sub(pattern, replacement, pyspark_expr, flags=re.IGNORECASE)
        
        return f'expr("""{pyspark_expr}""")  # Converted from: {expression[:50]}...'
    
    def _add_footer(self):
        """Add notebook footer"""
        self._add_section("Execution Summary", "Validate the conversion and run final checks")
        self._add_line('''# Final validation
print("=" * 60)
print("Alteryx to PySpark Conversion Complete")
print("=" * 60)
print(f"Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\\nPlease review all TODO comments and update:")
print("  1. Input data sources and paths")
print("  2. Join keys and DataFrame references")  
print("  3. Filter expressions")
print("  4. Output destinations")
print("=" * 60)''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Converter

# COMMAND ----------

# Parse the workflow
try:
    parser = AlteryxWorkflowParser(workflow_file_path)
    workflow = parser.parse()
    
    print(f"✅ Successfully parsed workflow: {workflow.name}")
    print(f"   Version: {workflow.version}")
    print(f"   Containers: {len(workflow.containers)}")
    print(f"   Standalone Tools: {len(workflow.tools)}")
    print(f"   Connections: {len(workflow.connections)}")
    print(f"   Constants: {len(workflow.constants)}")
    
except Exception as e:
    print(f"❌ Error parsing workflow: {str(e)}")
    raise

# COMMAND ----------

# Display workflow structure
print("\n📦 WORKFLOW STRUCTURE\n" + "=" * 50)

def print_container(container, indent=0):
    prefix = "  " * indent
    status = "🔴 DISABLED" if container.disabled else "🟢"
    print(f"{prefix}{status} Container: {container.caption} (ID: {container.tool_id})")
    print(f"{prefix}   Tools: {len(container.tools)}")
    
    for tool in container.tools[:3]:
        print(f"{prefix}   - {tool.tool_type} (ID: {tool.tool_id})")
    if len(container.tools) > 3:
        print(f"{prefix}   ... and {len(container.tools) - 3} more tools")
    
    for child in container.child_containers:
        print_container(child, indent + 1)

for container in workflow.containers:
    print_container(container)
    print()

# COMMAND ----------

# Display constants
if workflow.constants:
    print("\n⚙️ WORKFLOW CONSTANTS\n" + "=" * 50)
    for key, value in workflow.constants.items():
        print(f"  {key}: {value['value']} ({'numeric' if value['is_numeric'] else 'string'})")

# COMMAND ----------

# Generate PySpark code
generator = PySparkCodeGenerator(workflow)
generated_code = generator.generate()

print(f"\n✅ Generated {len(generated_code.split(chr(10))):,} lines of PySpark code")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated PySpark Code
# MAGIC
# MAGIC The code below is the converted PySpark notebook. Copy this to a new notebook or save it.

# COMMAND ----------

# Display the generated code
print(generated_code)

# COMMAND ----------

# Optionally save to a file
if generate_file:
    try:
        with open(output_notebook_path, 'w') as f:
            f.write(generated_code)
        print(f"✅ Saved generated notebook to: {output_notebook_path}")
    except Exception as e:
        print(f"⚠️ Could not save file: {str(e)}")
        print("You can copy the generated code from the cell above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Mapping Reference
# MAGIC
# MAGIC | Alteryx Tool | PySpark Equivalent |
# MAGIC |--------------|-------------------|
# MAGIC | Input Data | `spark.read.format().load()` or `.table()` |
# MAGIC | Select | `.select()`, `.drop()`, `.withColumnRenamed()` |
# MAGIC | Filter | `.filter(condition)` |
# MAGIC | Formula | `.withColumn("col", expression)` |
# MAGIC | Join | `.join(df, on="key", how="inner")` |
# MAGIC | Union | `.unionByName(df, allowMissingColumns=True)` |
# MAGIC | Summarize | `.groupBy().agg()` |
# MAGIC | Sort | `.orderBy(col.asc())` |
# MAGIC | Sample | `.sample(fraction)` or `.limit(n)` |
# MAGIC | Unique | `.dropDuplicates()` |
# MAGIC | Cross Tab | `.groupBy().pivot().agg()` |
# MAGIC | Text to Columns | `split()`, `.getItem()` |
# MAGIC | Find Replace | `.join()` with lookup table |
# MAGIC | Browse | `display()` or `.show()` |
# MAGIC | Output Data | `.write.format("delta").save()` |
# MAGIC | In-DB Tools | Standard PySpark operations (pushdown automatic) |
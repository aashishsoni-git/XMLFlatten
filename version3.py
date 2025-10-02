"""
Generic XML to Snowflake ETL - Denormalized Version
Author: Aashish
Description: Converts XML to denormalized Snowflake tables
- Only Level 1 children of <data> become tables
- Non-repetitive nodes are flattened
- Repetitive nodes create rows with parent data merged
"""

import xml.etree.ElementTree as ET
import pandas as pd
import snowflake.connector
from collections import defaultdict
import hashlib
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime
import configparser
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class XMLToSnowflakeETL:
    """Generic XML to Snowflake ETL with denormalized output"""
    
    def __init__(self, config_file: str = None, table_prefix: str = "", data_node: str = "data"):
        """
        Initialize ETL with Snowflake connection
        
        Args:
            config_file: Path to config file (INI format). If None, uses default config.ini
            table_prefix: Optional prefix for table names (e.g., 'STG_', 'RAW_')
            data_node: Name of the node whose children become tables (default: "data")
        """
        self.config_file = config_file or 'config.ini'
        self.sf_config = self._load_config()
        self.table_prefix = table_prefix
        self.data_node = data_node
        self.conn = None
        self.tables_metadata = {}
    
    def _load_config(self) -> Dict[str, str]:
        """Load Snowflake configuration from INI file"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Config file not found: {self.config_file}")
        
        config = configparser.RawConfigParser()
        config.read(self.config_file)
        
        if 'snowflake' not in config:
            raise ValueError("Config file must contain [snowflake] section")
        
        def clean_value(value):
            if value and len(value) >= 2:
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    return value[1:-1]
            return value
        
        sf_config = {
            'account': clean_value(config.get('snowflake', 'SNOWFLAKE_ACCOUNT')),
            'user': clean_value(config.get('snowflake', 'SNOWFLAKE_USER')),
            'password': clean_value(config.get('snowflake', 'SNOWFLAKE_PASSWORD')),
            'warehouse': clean_value(config.get('snowflake', 'SNOWFLAKE_WAREHOUSE')),
            'database': clean_value(config.get('snowflake', 'SNOWFLAKE_DATABASE')),
            'role': clean_value(config.get('snowflake', 'SNOWFLAKE_ROLE', fallback='ACCOUNTADMIN'))
        }
        
        sf_config['schema'] = clean_value(config.get('snowflake', 'SNOWFLAKE_SCHEMA', fallback='PUBLIC'))
        
        logger.info(f"Loaded config from {self.config_file}")
        return sf_config
        
    def connect(self):
        """Establish Snowflake connection"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.sf_config['user'],
                password=self.sf_config['password'],
                account=self.sf_config['account'],
                warehouse=self.sf_config['warehouse'],
                database=self.sf_config['database'],
                schema=self.sf_config['schema'],
                role=self.sf_config['role']
            )
            logger.info(f"✅ Connected to Snowflake - Database: {self.sf_config['database']}, Schema: {self.sf_config['schema']}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")
    
    def parse_xml_file(self, xml_file_path: str) -> ET.Element:
        """Parse XML file and return root element"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            logger.info(f"Successfully parsed XML file: {xml_file_path}")
            return root
        except Exception as e:
            logger.error(f"Failed to parse XML file: {e}")
            raise
    
    def _flatten_element(self, element: ET.Element, prefix: str = "") -> Dict[str, Any]:
        """Recursively flatten an XML element into a dictionary"""
        result = {}
        
        for attr_name, attr_value in element.attrib.items():
            key = f"{prefix}{element.tag}_{attr_name}" if prefix else f"{element.tag}_{attr_name}"
            result[key] = attr_value
        
        if element.text and element.text.strip():
            key = f"{prefix}{element.tag}" if prefix else element.tag
            result[key] = element.text.strip()
        
        child_tags = defaultdict(list)
        for child in element:
            child_tags[child.tag].append(child)
        
        for tag, children in child_tags.items():
            if len(children) > 1:
                return None
        
        for tag, children in child_tags.items():
            child = children[0]
            new_prefix = f"{prefix}{element.tag}_" if prefix else f"{element.tag}_"
            child_result = self._flatten_element(child, new_prefix)
            
            if child_result is None:
                return None
            
            result.update(child_result)
        
        return result
    
    def _has_repetitive_descendants(self, element: ET.Element) -> bool:
        """Check if element or any of its descendants have repetitive children"""
        child_tags = defaultdict(int)
        for child in element:
            child_tags[child.tag] += 1
        
        if any(count > 1 for count in child_tags.values()):
            return True
        
        for child in element:
            if self._has_repetitive_descendants(child):
                return True
        
        return False
    
    def _extract_repetitive_data(self, element: ET.Element, parent_data: Dict, 
                                 collected_data: List[Dict], table_name: str):
        """Recursively extract data, creating rows for repetitive elements"""
        child_tags = defaultdict(list)
        for child in element:
            child_tags[child.tag].append(child)
        
        repetitive = {}
        non_repetitive = {}
        
        for tag, children in child_tags.items():
            if len(children) > 1:
                repetitive[tag] = children
            else:
                non_repetitive[tag] = children[0]
        
        current_data = parent_data.copy()
        
        if element.text and element.text.strip():
            current_data[element.tag] = element.text.strip()
        
        for attr_name, attr_value in element.attrib.items():
            current_data[f"{element.tag}_{attr_name}"] = attr_value
        
        for tag, child in non_repetitive.items():
            flattened = self._flatten_element(child)
            if flattened:
                current_data.update(flattened)
            else:
                self._extract_repetitive_data(child, current_data, collected_data, table_name)
                return
        
        if repetitive:
            for tag, children in repetitive.items():
                for idx, child in enumerate(children):
                    row_data = current_data.copy()
                    row_data['_sequence_num'] = idx + 1
                    self._extract_repetitive_data(child, row_data, collected_data, table_name)
        else:
            if current_data != parent_data:
                row_data = current_data.copy()
                row_data['_load_timestamp'] = datetime.now().isoformat()
                collected_data.append(row_data)
    
    def flatten_xml(self, root: ET.Element) -> Dict[str, List[Dict]]:
        """Flatten XML into tables based on Level 1 children of <data> node"""
        tables = {}
        
        data_element = None
        if root.tag == self.data_node:
            data_element = root
        else:
            data_element = root.find(f".//{self.data_node}")
        
        if data_element is None:
            logger.warning(f"Could not find '{self.data_node}' node in XML")
            return tables
        
        for child in data_element:
            table_name = child.tag
            logger.info(f"Processing Level 1 node: {table_name}")
            
            collected_data = []
            has_repetitive = self._has_repetitive_descendants(child)
            
            if has_repetitive:
                parent_data = {}
                
                if child.text and child.text.strip():
                    parent_data[child.tag] = child.text.strip()
                
                for attr_name, attr_value in child.attrib.items():
                    parent_data[f"{child.tag}_{attr_name}"] = attr_value
                
                self._extract_repetitive_data(child, parent_data, collected_data, table_name)
            else:
                flattened = self._flatten_element(child)
                if flattened:
                    flattened['_load_timestamp'] = datetime.now().isoformat()
                    collected_data.append(flattened)
            
            if collected_data:
                tables[table_name] = collected_data
                logger.info(f"  → Generated {len(collected_data)} rows for {table_name}")
        
        return tables
    
    def _infer_snowflake_type(self, value: Any) -> str:
        """Infer Snowflake data type from Python value"""
        if value is None or value == '':
            return 'VARCHAR(500)'
        
        value_str = str(value).strip()
        
        try:
            int(value_str)
            return 'INTEGER'
        except ValueError:
            pass
        
        try:
            float(value_str)
            return 'FLOAT'
        except ValueError:
            pass
        
        if len(value_str) in [8, 10] and (value_str.count('-') == 2 or value_str.count('/') == 2):
            return 'DATE'
        
        max_len = max(len(value_str), 100)
        return f'VARCHAR({min(max_len * 2, 16777216)})'
    
    def _create_table_if_not_exists(self, table_name: str, sample_record: Dict):
        """Dynamically create Snowflake table based on record structure"""
        full_table_name = f"{self.table_prefix}{table_name}".upper()
        
        cursor = self.conn.cursor()
        
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{self.sf_config['schema'].upper()}' 
                AND table_name = '{full_table_name}'
            """)
            
            if cursor.fetchone()[0] > 0:
                logger.info(f"Table {full_table_name} already exists")
                return
            
            columns = []
            for col_name, col_value in sample_record.items():
                col_type = self._infer_snowflake_type(col_value)
                safe_col_name = col_name.replace('-', '_').replace('.', '_').upper()
                columns.append(f"{safe_col_name} {col_type}")
            
            create_stmt = f"CREATE TABLE {full_table_name} ({', '.join(columns)})"
            cursor.execute(create_stmt)
            logger.info(f"✅ Created table: {full_table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create table {full_table_name}: {e}")
            raise
        finally:
            cursor.close()
    
    def _load_data_to_table(self, table_name: str, records: List[Dict]):
        """Load records into Snowflake table"""
        if not records:
            logger.warning(f"No records to load for table {table_name}")
            return
        
        full_table_name = f"{self.table_prefix}{table_name}".upper()
        df = pd.DataFrame(records)
        
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        df.columns = [col.replace('-', '_').replace('.', '_').upper() for col in df.columns]
        
        cursor = self.conn.cursor()
        
        try:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{self.sf_config['schema'].upper()}' 
                AND table_name = '{full_table_name}'
            """)
            existing_cols = set([row[0] for row in cursor.fetchall()])
            
            for col in df.columns:
                if col not in existing_cols:
                    alter_stmt = f"ALTER TABLE {full_table_name} ADD COLUMN {col} VARCHAR(500)"
                    cursor.execute(alter_stmt)
                    logger.info(f"Added column {col} to {full_table_name}")
            
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_stmt = f"INSERT INTO {full_table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_stmt, df.values.tolist())
            self.conn.commit()
            
            logger.info(f"✅ Loaded {len(records)} records into {full_table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data into {full_table_name}: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def process_xml_file(self, xml_file_path: str):
        """Main method to process XML file end-to-end"""
        logger.info(f"🚀 Starting ETL process for {xml_file_path}")
        
        try:
            root = self.parse_xml_file(xml_file_path)
            tables = self.flatten_xml(root)
            
            logger.info(f"📊 Generated {len(tables)} tables from XML structure")
            
            for table_name, records in tables.items():
                if records:
                    self._create_table_if_not_exists(table_name, records[0])
                    self._load_data_to_table(table_name, records)
            
            logger.info("✅ ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"❌ ETL process failed: {e}")
            raise


if __name__ == "__main__":
    etl = XMLToSnowflakeETL(
        config_file='config.ini',
        table_prefix="",
        data_node="data"
    )
    
    try:
        etl.connect()
        etl.process_xml_file('insurance_data.xml')
    finally:
        etl.disconnect()
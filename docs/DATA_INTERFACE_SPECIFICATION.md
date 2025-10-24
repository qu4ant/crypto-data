# Binance Dataset Generator - Data Interface Specification

## Overview

This specification defines a new high-cohesion architecture for generating clean, merged datasets from Binance historical data. The system follows SOLID principles with clear separation of concerns between data reading, cleaning, and merging operations.

## Architecture Design

### Core Components

```
BinanceDatasetBuilder (Orchestrator)
    ├── BinanceDataReader (Existing - Data Loading)
    ├── BinanceDataCleaner (New - Data Quality)
    ├── BinanceDataMerger (New - Data Integration)
    └── BinanceDatasetConfig (New - Configuration Management)
```

### Component Responsibilities

#### 1. BinanceDatasetBuilder (Main Orchestrator)
**Single Responsibility**: Coordinate the entire dataset creation pipeline
- **Input**: Configuration object or parameters
- **Output**: Clean, merged DataFrame ready for analysis
- **Responsibilities**:
  - Validate configuration
  - Coordinate reader, cleaner, merger components
  - Handle high-level error management
  - Provide progress reporting
  - Manage pipeline execution flow

#### 2. BinanceDataCleaner (Data Quality Management)
**Single Responsibility**: Clean and validate data quality
- **Input**: Raw DataFrames from reader
- **Output**: Clean DataFrames with quality reports
- **Responsibilities**:
  - Remove duplicate rows based on timestamp
  - Handle NaN values (multiple strategies)
  - Validate data completeness and consistency
  - Generate data quality reports
  - Log cleaning operations

#### 3. BinanceDataMerger (Data Integration)
**Single Responsibility**: Merge and align different data types
- **Input**: Clean DataFrames from cleaner
- **Output**: Merged DataFrame with aligned timestamps
- **Responsibilities**:
  - Merge multiple data types on timestamps
  - Forward fill funding rates (8h intervals → 5m intervals)
  - Handle different data availability scenarios
  - Remove incomplete rows based on requirements
  - Align timestamps across data types

#### 4. BinanceDatasetConfig (Configuration Management)
**Single Responsibility**: Manage and validate configuration
- **Input**: Configuration parameters or files
- **Output**: Validated configuration object
- **Responsibilities**:
  - Type validation and defaults
  - Configuration serialization/deserialization
  - Parameter validation and normalization
  - Documentation of all options

## API Interface Design

### High-Level Usage

```python
from binance_data.dataset import BinanceDatasetBuilder, BinanceDatasetConfig

# Method 1: Direct parameters
builder = BinanceDatasetBuilder(data_directory="./binance_data_files")
dataset = builder.build_dataset(
    symbols=['BTCUSDT', 'ETHUSDT'],
    data_types=['spot', 'futures', 'funding_rate'],
    interval='5m',
    start_date='2024-01-01',
    end_date='2024-03-31',
    cleaning_options={
        'remove_duplicates': True,
        'nan_strategy': 'drop',
        'remove_incomplete_rows': True
    },
    merge_options={
        'funding_fill_method': 'forward',
        'alignment': 'inner',
        'max_fill_gap': '4h'
    }
)

# Method 2: Configuration object
config = BinanceDatasetConfig(
    symbols=['BTCUSDT', 'ETHUSDT'],
    data_types=['spot', 'futures', 'funding_rate'],
    interval='5m',
    date_range=('2024-01-01', '2024-03-31'),
    cleaning=CleaningConfig(
        remove_duplicates=True,
        nan_strategy='drop',
        remove_incomplete_rows=True
    ),
    merging=MergingConfig(
        funding_fill_method='forward',
        alignment='inner',
        max_fill_gap='4h'
    )
)

dataset = builder.build_dataset(config=config)

# Method 3: Configuration file
config = BinanceDatasetConfig.from_yaml('config/dataset_config.yaml')
dataset = builder.build_dataset(config=config)
```

### Configuration Schema

```yaml
# dataset_config.yaml
symbols:
  - BTCUSDT
  - ETHUSDT
  - ADAUSDT

data_types:
  - spot
  - futures
  - premium_index
  - funding_rate

interval: '5m'  # Target interval for final dataset

date_range:
  start: '2024-01-01'
  end: '2024-03-31'

cleaning:
  remove_duplicates: true
  nan_strategy: 'drop'  # 'drop', 'fill', 'interpolate'
  nan_fill_method: 'forward'  # when nan_strategy = 'fill'
  remove_incomplete_rows: true
  max_consecutive_nan: 10

merging:
  alignment: 'inner'  # 'inner', 'outer', 'left', 'right'
  funding_fill_method: 'forward'  # How to fill funding rates to match interval
  max_fill_gap: '4h'  # Maximum gap to forward fill
  require_all_types: true  # Remove rows where any data type is missing

output:
  format: 'dataframe'  # 'dataframe', 'csv', 'parquet'
  file_path: null  # Optional output file path
  include_metadata: true
```

## Component Detailed Specifications

### BinanceDataCleaner

```python
class BinanceDataCleaner:
    """Clean and validate Binance data with comprehensive quality control"""

    def clean_dataframe(self, df: pd.DataFrame, data_type: str,
                       config: CleaningConfig) -> CleanedDataResult:
        """Clean a single DataFrame"""

    def remove_duplicates(self, df: pd.DataFrame,
                         subset: list[str] = None) -> pd.DataFrame:
        """Remove duplicate rows based on timestamp or subset"""

    def handle_nan_values(self, df: pd.DataFrame,
                         strategy: str, method: str = None) -> pd.DataFrame:
        """Handle NaN values with specified strategy"""

    def validate_data_completeness(self, df: pd.DataFrame,
                                  data_type: str) -> ValidationReport:
        """Validate data completeness and consistency"""

    def generate_quality_report(self, original_df: pd.DataFrame,
                               cleaned_df: pd.DataFrame) -> QualityReport:
        """Generate detailed quality report"""
```

### BinanceDataMerger

```python
class BinanceDataMerger:
    """Merge and align different Binance data types"""

    def merge_datasets(self, datasets: dict[str, pd.DataFrame],
                      config: MergingConfig) -> MergedDataResult:
        """Merge multiple datasets with alignment"""

    def forward_fill_funding(self, funding_df: pd.DataFrame,
                            target_interval: str, max_gap: str) -> pd.DataFrame:
        """Forward fill funding rates to match target interval"""

    def align_timestamps(self, datasets: dict[str, pd.DataFrame],
                        alignment: str) -> dict[str, pd.DataFrame]:
        """Align timestamps across all datasets"""

    def filter_incomplete_rows(self, merged_df: pd.DataFrame,
                              required_types: list[str]) -> pd.DataFrame:
        """Remove rows where required data types are missing"""
```

### BinanceDatasetBuilder

```python
class BinanceDatasetBuilder:
    """Main orchestrator for dataset creation"""

    def __init__(self, data_directory: str = "./binance_data_files"):
        self.reader = BinanceDataReader(data_directory)
        self.cleaner = BinanceDataCleaner()
        self.merger = BinanceDataMerger()

    def build_dataset(self,
                     symbols: list[str] = None,
                     data_types: list[str] = None,
                     interval: str = '5m',
                     start_date: str = None,
                     end_date: str = None,
                     cleaning_options: dict = None,
                     merge_options: dict = None,
                     config: BinanceDatasetConfig = None) -> DatasetResult:
        """Build complete dataset with cleaning and merging"""

    def validate_configuration(self, config: BinanceDatasetConfig) -> ValidationResult:
        """Validate configuration before processing"""

    def get_progress_callback(self) -> callable:
        """Return progress callback for monitoring"""
```

## Data Flow Pipeline

```
1. Configuration Validation
   ├── Validate symbols, dates, intervals
   ├── Check data availability
   └── Set defaults

2. Data Loading (per symbol)
   ├── Load spot klines
   ├── Load futures klines
   ├── Load premium index
   └── Load funding rates

3. Data Cleaning (per data type)
   ├── Remove duplicates
   ├── Handle NaN values
   ├── Validate completeness
   └── Generate quality report

4. Data Merging
   ├── Align timestamps
   ├── Forward fill funding rates (8h → 5m)
   ├── Merge on timestamp index
   └── Filter incomplete rows

5. Output Generation
   ├── Final validation
   ├── Generate metadata
   └── Export (DataFrame/CSV/Parquet)
```

## Error Handling Strategy

### Error Categories

1. **Configuration Errors**: Invalid parameters, missing files
2. **Data Quality Errors**: Corrupt files, inconsistent formats
3. **Processing Errors**: Memory issues, computation failures
4. **Integration Errors**: Misaligned timestamps, missing data

### Error Handling Approach

```python
class DatasetBuilderError(Exception):
    """Base exception for dataset builder"""

class ConfigurationError(DatasetBuilderError):
    """Configuration validation errors"""

class DataQualityError(DatasetBuilderError):
    """Data quality issues"""

class ProcessingError(DatasetBuilderError):
    """Processing and computation errors"""

class IntegrationError(DatasetBuilderError):
    """Data integration and merging errors"""
```

## Testing Strategy

### Unit Tests
- `test_cleaner.py`: Test cleaning operations independently
- `test_merger.py`: Test merging logic with mock data
- `test_config.py`: Test configuration validation
- `test_builder.py`: Test orchestration logic

### Integration Tests
- `test_dataset_integration.py`: Test full pipeline with real data
- `test_edge_cases.py`: Test error conditions and edge cases
- `test_performance.py`: Test with large datasets

### Test Data
- Sample data files for each data type
- Edge cases: missing data, duplicates, NaN values
- Performance datasets: large date ranges

## Usage Examples

### Basic Dataset Creation
```python
# Simple spot + futures dataset
dataset = builder.build_dataset(
    symbols=['BTCUSDT'],
    data_types=['spot', 'futures'],
    interval='5m',
    start_date='2024-01-01',
    end_date='2024-01-31'
)
```

### Advanced Configuration
```python
# Complex dataset with all data types
config = BinanceDatasetConfig(
    symbols=['BTCUSDT', 'ETHUSDT', 'ADAUSDT'],
    data_types=['spot', 'futures', 'premium_index', 'funding_rate'],
    interval='5m',
    date_range=('2024-01-01', '2024-03-31'),
    cleaning=CleaningConfig(
        remove_duplicates=True,
        nan_strategy='interpolate',
        max_consecutive_nan=5,
        remove_incomplete_rows=False
    ),
    merging=MergingConfig(
        alignment='outer',
        funding_fill_method='forward',
        max_fill_gap='6h',
        require_all_types=False
    ),
    output=OutputConfig(
        format='parquet',
        file_path='./datasets/crypto_data_Q1_2024.parquet',
        include_metadata=True
    )
)

result = builder.build_dataset(config=config)
```

### Batch Processing
```python
# Process multiple symbols in batches
symbols = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'BNBUSDT', 'SOLUSDT']
for symbol_batch in chunked(symbols, 2):
    dataset = builder.build_dataset(
        symbols=symbol_batch,
        data_types=['spot', 'futures'],
        start_date='2024-01-01',
        end_date='2024-12-31'
    )
    dataset.to_parquet(f'./datasets/{"-".join(symbol_batch)}_2024.parquet')
```

## Performance Considerations

### Memory Management
- Process symbols in batches to limit memory usage
- Use chunked reading for large date ranges
- Implement lazy loading for optional data types

### Optimization Strategies
- Vectorized operations for cleaning and merging
- Parallel processing for multiple symbols
- Efficient timestamp alignment algorithms
- Caching of intermediate results


## Migration Path

### Phase 1: Core Implementation
1. Implement `BinanceDataCleaner`
2. Implement `BinanceDataMerger`
3. Implement `BinanceDatasetConfig`
4. Create basic `BinanceDatasetBuilder`

### Phase 2: Advanced Features
1. Add configuration file support
2. Implement progress reporting
3. Add export format options
4. Create comprehensive test suite

### Phase 3: Performance & Scale
1. Implement parallel processing

This specification provides a solid foundation for building a robust, maintainable dataset generation system that follows clean architecture principles and provides maximum flexibility for users.
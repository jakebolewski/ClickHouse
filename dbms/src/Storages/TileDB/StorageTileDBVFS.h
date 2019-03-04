#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB {

/**
 * This class represents a table engine for external TileDB VFS files.
 */
class StorageTileDBVFS : public ext::shared_ptr_helper<StorageTileDBVFS>, public IStorage {
 public:
  String getName() const override {
    return "TileDBVFS";
  }

  String getTableName() const override
  {
    return table_name;
  }

  BlockInputStreams read(const Names & column_names,
      const SelectQueryInfo & query_info,
      const Context & context,
      QueryProcessingStage::Enum processed_stage,
      size_t max_block_size,
      unsigned num_streams) override;

 protected:
  StorageTileDBVFS(const String & uri_,
      const String & table_name_,
      const String & format_name_,
      const ColumnsDescription & columns_,
      Context & context_);

 private:
  String uri;
  String format_name;
  String table_name;
  Context & context;

  Logger * log = &Logger::get("StorageTileDBVFS");
};
}

#include <Storages/StorageFactory.h>
#include <Storages/TileDB/StorageTileDBVFS.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromTileDBVFS.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>

namespace DB {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageTileDBVFS::StorageTileDBVFS(const DB::String &uri_,
                                   const DB::String &table_name_,
                                   const DB::String &format_name_,
                                   const DB::ColumnsDescription &columns_,
                                   DB::Context &context_)
    : IStorage(columns_), uri(uri_), format_name(format_name_), table_name(table_name_), context(context_) {
}

namespace {

class TileDBVFSBlockInputStream : public IBlockInputStream {
 public:
  TileDBVFSBlockInputStream(const String &uri,
                            const String &format,
                            const Block &sample_block,
                            const Context &context,
                            UInt64 max_block_size) {
    std::unique_ptr<ReadBuffer> read_buf = std::make_unique<ReadBufferFromTileDBVFS>(uri);
    auto input_stream = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
    reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(read_buf));
  }

  String getName() const override {
    return "TileDBVFS";
  }

  Block readImpl() override {
    return reader->read();
  }

  Block getHeader() const override {
    return reader->getHeader();
  }

  void readPrefixImpl() override {
    reader->readPrefix();
  }

  void readSuffixImpl() override {
    reader->readSuffix();
  }

 private:
  BlockInputStreamPtr reader;
};

}

BlockInputStreams StorageTileDBVFS::read(
    const Names &,
    const SelectQueryInfo &,
    const Context &context_,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    unsigned) {
  return {std::make_shared<TileDBVFSBlockInputStream>(
      uri,
      format_name,
      getSampleBlock(),
      context_,
      max_block_size)};
}

void registerStorageTileDBVFS(StorageFactory & factory)
{
  factory.registerStorage("TileDBVFS", [](const StorageFactory::Arguments &args)
  {
    ASTs &engine_args = args.engine_args;

    if (!(engine_args.size() == 1 || engine_args.size() == 2))
      throw Exception(
          "Storage TileDBVFS requires exactly 2 arguments: uri and name of used format.",
          ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

    String url = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();

    engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

    String format_name = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

    return StorageTileDBVFS::create(url, args.table_name, format_name, args.columns, args.context);
  });
}
}
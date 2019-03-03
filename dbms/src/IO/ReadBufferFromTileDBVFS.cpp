#include <Common/config.h>
#include <IO/ReadBufferFromTileDBVFS.h> // Y_IGNORE
#include <Poco/URI.h>
#include <tiledb/tiledb>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

struct ReadBufferFromTileDBVFS::ReadBufferFromTileDBVFSImpl
{
    Poco::URI tiledb_uri_;
    tiledb::Context ctx_;
    tiledb::VFS vfs_;
    tiledb::VFS::filebuf buff_;
    std::istream istream_;

    explicit ReadBufferFromTileDBVFSImpl(const std::string & uri_)
        : tiledb_uri_(uri_)
        , vfs_(ctx_)
        , buff_(vfs_)
        , istream_(&buff_)
    {
        tiledb_uri_.normalize();
        buff_.open(tiledb_uri_.toString().c_str(), std::ios::in);
        if (!istream_.good()) {
          throw Exception("Unable to open TileDB VFS file: " + tiledb_uri_.toString(),
                ErrorCodes::CANNOT_OPEN_FILE);
        }
    }

    size_t read(char * start, size_t size) {
        istream_.read(start, size);
        std::streamsize gcount = istream_.gcount();
        if (!gcount) {
            if (istream_.eof()) {
                return false;
            }
            std::streampos pos = istream_.tellg();
            if (istream_.fail())
                throw Exception("Cannot read from TileDB VFS filebuf at offset " + std::to_string(pos), ErrorCodes::CANNOT_READ_ALL_DATA);
            throw Exception("Unexpected state of TileDB VFS filebuf at offset " + std::to_string(pos), ErrorCodes::CANNOT_READ_ALL_DATA);
        }
        return static_cast<size_t>(gcount);
    }

    ~ReadBufferFromTileDBVFSImpl()
    {
      buff_.close();
    }
};

ReadBufferFromTileDBVFS::ReadBufferFromTileDBVFS(const std::string & uri_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size)
    , impl(std::make_unique<ReadBufferFromTileDBVFSImpl>(uri_))
{
}

bool ReadBufferFromTileDBVFS::nextImpl()
{
    size_t bytes_read = impl->read(internal_buffer.begin(), internal_buffer.size());
    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;
    return true;
}

ReadBufferFromTileDBVFS::~ReadBufferFromTileDBVFS(){
}

}

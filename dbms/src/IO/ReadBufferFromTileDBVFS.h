#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>

namespace DB
{
/** Accepts TileDB VFS URI path to file and opens it.
 * Closes file, thus "owns" a VFS file.
 */
class ReadBufferFromTileDBVFS : public BufferWithOwnMemory<ReadBuffer>
{
    struct ReadBufferFromTileDBVFSImpl;
    std::unique_ptr<ReadBufferFromTileDBVFSImpl> impl;
public:
    ReadBufferFromTileDBVFS(const std::string & uri_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    ReadBufferFromTileDBVFS(ReadBufferFromTileDBVFS &&) = default;

    bool nextImpl() override;

    ~ReadBufferFromTileDBVFS() override;
};
}

#include <string>

#include <iostream>
#include <sstream>

#include <Core/Types.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromTileDBVFS.h>

int main(int, char **)
{
  try
  {
    DB::ReadBufferFromTileDBVFS in("file:///Users/jacobbolewski/TileDB/ClickHouse/dbms/src/IO/tests/tiledb_vfs_read.cpp");
    DB::String res;
    {
        DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        DB::copyData(in, out);
    }
  }
  catch (const DB::Exception & e)
  {
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
  }

  return 0;
}

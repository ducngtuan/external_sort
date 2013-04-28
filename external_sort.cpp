#include "external_sort.h"

#include <algorithm>
#include <array>
#include <iostream>
#include <memory>
#include <queue>
#include <vector>
#include <string>

#include <cstdint>
#include <cstring>
#include <cerrno>

// headers needed for working with files
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#ifdef __linux__
  #include <linux/falloc.h>
#endif

// Maximum numbers of chunks to merge in one pass
const unsigned long kMaxNumberOfChunksForOnePass = 50;

// Custom compare class to use in the priority queue.
// This compare make sure the queue in reverse order (min element at top).
class PairCompare {
public:
  bool operator()(const std::pair<uint64_t, size_t>& lhs,
                  const std::pair<uint64_t, size_t>& rhs) {
    return lhs.first > rhs.first;
  }
};

// Utility functions
bool preallocate_file(int fd, off_t offset = 0, off_t len = 0);

void delete_temps(const std::vector<std::string>& tempfiles);

std::vector<std::string> split_file(int fd, unsigned long chunk_size);

void merge_one_pass(std::vector<std::string> chunks, int fd_output,
                    unsigned long mem_size);

void fill_buffer(const int fd, const std::unique_ptr<uint64_t[]> &buffer,
                 const size_t buffer_size, size_t &max_idx, size_t &idx);

void flush_buffer(const int fd, const std::unique_ptr<uint64_t[]> &buffer,
                  const size_t buffer_len);

///
/// K-way merge external sort
///
/// \param fdInput  descriptor of input file
/// \param size     total number of integers
/// \param fdOutput descriptor of output file
/// \param memSize  size of main memory in bytes
///
void externalSort(int fdInput, unsigned long size,
                  int fdOutput, unsigned long memSize) {
  auto chunk_len = memSize / sizeof(uint64_t);
  auto fd_temps = split_file(fdInput, chunk_len);

  preallocate_file(fdOutput, 0, size * sizeof(uint64_t));

  merge_one_pass(fd_temps, fdOutput, memSize);
}

///
/// Split the input file in small sorted chunk
///
/// \param fd_input  descriptor of the input file
/// \param chunk_len number of integers in one chunk
/// \return list of all the chunk files
///
std::vector<std::string> split_file(int fd_input, unsigned long chunk_len) {
  auto buffer      = std::unique_ptr<uint64_t[]>(new uint64_t[chunk_len]);
  auto buffer_size = chunk_len * sizeof(uint64_t);
  auto temp_files  = std::vector<std::string>();
  auto len = 0L;

  while (true) {
    len = read(fd_input, buffer.get(), buffer_size);
    if (len <= 0) break;

    // sort the buffer
    std::sort(&buffer[0], &buffer[0] + len/sizeof(uint64_t));

    // write buffer to temporal output file
    char temp[] = "esort.tmp.XXXXXX";
    int fd_temp = mkstemp(temp);
    if (fd_temp == -1) {
      std::cerr << "Error: cannot creating temporary file - "
                << strerror(errno) << std::endl;
      break;
    }
    preallocate_file(fd_temp, 0, len);

    write(fd_temp, buffer.get(), len);
    close(fd_temp);
    temp_files.push_back(std::string(temp));
  }

  return temp_files;
}

///
/// Merge all the chunks to the output file, in one pass.
///
/// \param chunks     list of filename of the chunks
/// \param fd_output  file descriptor of the output file
/// \param chunk_size size of each chunk
/// \param mem_size   size of main memory
///
void merge_one_pass(std::vector<std::string> chunks, int fd_output,
                    unsigned long mem_size) {
  // prepare the input and output buffers
  auto buffer_len = mem_size / (chunks.size() + 1) / sizeof(uint64_t);
  auto buffer_size = buffer_len * sizeof(uint64_t);

  std::unique_ptr<uint64_t[]> out_buffer(new uint64_t[buffer_len]);
  size_t out_idx = 0;

  std::vector<std::unique_ptr<uint64_t[]>> in_buffers;
  std::vector<int> fds;
  for (auto& chunk : chunks) {
    fds.push_back(open(chunk.c_str(), O_RDONLY));
    in_buffers.push_back(std::unique_ptr<uint64_t[]>(new uint64_t[buffer_len]));
  }

  std::vector<size_t> max_idx(chunks.size(), buffer_len);
  std::vector<size_t> idx(chunks.size(), 0);

  for (size_t i = 0; i != chunks.size(); ++i)
    fill_buffer(fds[i], in_buffers[i], buffer_size, max_idx[i], idx[i]);

  std::priority_queue<std::pair<uint64_t, size_t>,
      std::vector<std::pair<uint64_t, size_t>>,
      PairCompare> queue;
  // fill queue with one item from each buffer
  for (size_t i = 0; i != in_buffers.size(); ++i)
    queue.push(std::make_pair(in_buffers[i][idx[i]++], i));

  while (!queue.empty()) {
    auto p = queue.top();
    queue.pop();

    out_buffer[out_idx++] = p.first;
    // flush the output buffer if full
    if (out_idx == buffer_len) {
      flush_buffer(fd_output, out_buffer, out_idx);
      out_idx = 0;
    }

    // take the next element from the readed input buffer
    size_t i = p.second;

    if (idx[i] == max_idx[i])
      fill_buffer(fds[i], in_buffers[i], buffer_size, max_idx[i], idx[i]);
    if (idx[i] == max_idx[i]) continue;

    queue.push(std::make_pair(in_buffers[i][idx[i]++], i));
  }
  flush_buffer(fd_output, out_buffer, out_idx);
  delete_temps(chunks);
}

///
/// Fill the buffer with the content from the input file.
///
/// \param fd     file descriptor of the input file
/// \param buffer pointer to the buffer array
/// \param buffer_size number of bytes to read
/// \param max_idx length of the new buffer after read
/// \param idx     current position of the buffer, will be reset to 0
///
void fill_buffer(const int fd, const std::unique_ptr<uint64_t[]>& buffer,
                 const size_t buffer_size, size_t& max_idx, size_t& idx) {
  auto len = read(fd, buffer.get(), buffer_size);
  max_idx = (len <= 0) ? 0 : (len / sizeof(uint64_t));
  idx = 0;
}

///
/// Write the content of the buffer to its output file.
///
/// \param fd         file descriptor of the output file
/// \param buffer     pointer to the buffer array
/// \param buffer_len length of the buffer to write to output
///
void flush_buffer(const int fd, const std::unique_ptr<uint64_t[]>& buffer,
                  const size_t buffer_len) {
  auto size = buffer_len * sizeof(uint64_t);
  auto len = write(fd, buffer.get(), size);
  if (len == -1)
    std::cerr << "Error: cannot write buffer to output file - "
              << strerror(errno) << std::endl;
}

///
/// Pre-allocate the file with the given size.
/// This function will call `fallocate` if the code
/// is built on Linux, `posix_fallocate` if the code
/// is built on Unix, and `fcntl` if the code built on Mac OS X.
///
/// \param fd     file descriptor
/// \param offset starting position
/// \param len    number of bytes since the offset
/// \return `true` on success, else `false`
///
bool preallocate_file(int fd, off_t offset, off_t len) {
  bool success = false;
#ifdef __linux__
  success = 0 == fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, len);
#elif _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112
  success = 0 == posix_fallocate(fd, offset, len);
#elif defined(__APPLE__) && defined(__MACH__)
  fstore_t fst {F_ALLOCATECONTIG, F_PEOFPOSMODE, offset, len};
  success = -1 != fcntl(fd, F_PREALLOCATE, &fst);
#endif

  if (!success)
    std::cerr << "Warning: cannot pre-allocate output file with size "
              << (len - offset) << " bytes" << std::endl;
  return success;
}

///
/// Delete all temporary file in the given list
///
/// \param tempfiles list of the name of files to be removed
///
void delete_temps(const std::vector<std::string>& tempfiles) {
  for (auto& temp : tempfiles) {
    if (remove(temp.c_str()) != 0) {
      std::cerr << "Error: cannot remove file '" << temp << "' - "
                << strerror(errno) << std::endl;
    }
  }
}

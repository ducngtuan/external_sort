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

///
/// Custom compare class to use in the priority queue. This implementation
/// makes sure the queue in reverse order (min element at top).
///
class PairCompare {
public:
  bool operator()(const std::pair<uint64_t, size_t>& lhs,
                  const std::pair<uint64_t, size_t>& rhs) {
    return lhs.first > rhs.first;
  }
};

// Utility functions
bool preallocate_file(int fd, off_t offset = 0, off_t len = 0);

void delete_files(const std::vector<std::string>& tempfiles);

std::vector<std::string> split_file(const int fd_input,
                                    const unsigned long chunk_size);

void merge_one_pass(const std::vector<std::string>& chunks,
                    const int fd_output,
                    const unsigned long mem_size);

void merge_multi_pass(std::vector<std::string>& chunks,
                      const int fd_output,
                      size_t chunk_size,
                      const unsigned long mem_size,
                      const int max_chunk_num = 25);

void fill_buffer(const int fd, const std::unique_ptr<uint64_t[]> &buffer,
                 const size_t buffer_size, size_t &max_idx, size_t &idx);

void flush_buffer(const int fd, const std::unique_ptr<uint64_t[]> &buffer,
                  const size_t buffer_len);

///
/// K-way merge external sort
///
/// \param fd_input  descriptor of input file
/// \param size      total number of integers
/// \param fd_output descriptor of output file
/// \param mem_size  size of main memory in bytes
///
void externalSort(const int fd_input, const unsigned long size,
                  const int fd_output, const unsigned long mem_size) {
  auto chunk_len = mem_size / sizeof(uint64_t);
  auto chunks = split_file(fd_input, chunk_len);

  preallocate_file(fd_output, 0, size * sizeof(uint64_t));

  merge_multi_pass(chunks, fd_output, chunk_len * sizeof(uint64_t), mem_size);
}

///
/// Split the input file in small sorted chunk
///
/// \param fd_input  descriptor of the input file
/// \param chunk_len number of integers in one chunk
/// \return list of all the chunk files
///
std::vector<std::string> split_file(const int fd_input,
                                    const unsigned long chunk_len) {
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
/// Perform multi-pass k-way merge. For each pass only merge `max_chunk_num`
/// chunks at once.
///
/// \param chunks     list of filenames of the chunks
/// \param fd_output  file descriptor of the final output file
/// \param chunk_size size of each chunk in bytes (to pre-allocate)
/// \param mem_size   size of main memory buffer
/// \param max_chunk_num number of chunks to merge at once, default 25
///
void merge_multi_pass(std::vector<std::string>& chunks,
                      const int fd_output,
                      size_t chunk_size,
                      const unsigned long mem_size,
                      const int max_chunk_num) {
  while (!chunks.empty()) {
    // if all chunks fit in
    if (chunks.size() <= max_chunk_num) {
      merge_one_pass(chunks, fd_output, mem_size);
      return;
    }

    std::vector<std::string> new_chunks;
    auto it = chunks.begin();
    while (it != chunks.end()) {
      auto ite = it + max_chunk_num;
      if (ite > chunks.end()) ite = chunks.end();
      std::vector<std::string> sub_chunks(it, ite);

      // write buffer to temporal output file
      char temp[] = "esort.tmp.XXXXXX";
      int fd_temp = mkstemp(temp);
      if (fd_temp == -1) {
        std::cerr << "Error: cannot creating temporary file - "
                  << strerror(errno) << std::endl;
        break;
      }
      preallocate_file(fd_temp, 0, chunk_size * (ite - it));
      merge_one_pass(sub_chunks, fd_temp, mem_size);
      new_chunks.push_back(std::string(temp));
      it = ite;
    }

    chunk_size *= max_chunk_num;
    chunks = std::move(new_chunks);
  }
}

///
/// Merge all the chunks to the output file, all together in one pass.
///
/// \param chunks     list of filename of the chunks
/// \param fd_output  file descriptor of the output file
/// \param chunk_size size of each chunk
/// \param mem_size   size of main memory
///
void merge_one_pass(const std::vector<std::string>& chunks,
                    const int fd_output,
                    const unsigned long mem_size) {
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

  // create and fill priority queue with one item from each buffer
  std::priority_queue<std::pair<uint64_t, size_t>,
      std::vector<std::pair<uint64_t, size_t>>,
      PairCompare> queue;
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
    if (idx[i] == max_idx[i]) continue; // nothing more to read from the chunk

    queue.push(std::make_pair(in_buffers[i][idx[i]++], i));
  }
  flush_buffer(fd_output, out_buffer, out_idx);
  close(fd_output);

  for (auto& fd : fds)
    close(fd);
  delete_files(chunks);
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
/// \return true on success
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
/// Delete all files in the given list
///
/// \param files list of the name of files to be removed
///
void delete_files(const std::vector<std::string>& files) {
  for (auto& file : files) {
    if (remove(file.c_str()) != 0) {
      std::cerr << "Error: cannot remove file '" << file << "' - "
                << strerror(errno) << std::endl;
    }
  }
}

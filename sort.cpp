#include <iostream>

#include <cstdint>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "external_sort.h"

void verify(char *input) {
  std::cout << "Start verifying..." << std::endl;
  int fd_input = open(input, O_RDONLY);
  uint64_t last, curr;
  size_t count = 1;
  bool failed = false;
  read(fd_input, &last, sizeof(uint64_t));
  while (read(fd_input, &curr, sizeof(uint64_t)) > 0) {
    if (curr <= last)
      failed = true;
    last = curr;
    ++count;
  }
  std::cout << "Output file has total " << count << " integers" << std::endl;
  if (failed)
    std::cout << "Sort failed!" << std::endl;
  else
    std::cout << "Output file sorted correctly." << std::endl;
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << " <input file> <output file> "
              << "<memory buffer size in MB>" << std::endl;
    return -1;
  }

  int fd_input = open(argv[1], O_RDONLY);
  if (fd_input == -1) {
    std::cerr << "Cannot open input file '" << argv[1] << "' - "
              << strerror(errno) << std::endl;
    return -1;
  }

  int fd_output = open(argv[2], O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if (fd_output == -1) {
    std::cerr << "Cannot open output file '" << argv[2] << "' - "
              << strerror(errno) << std::endl;
    return -1;
  }

  int n = atoi(argv[3]);
  if (n == 0) {
    std::cerr << "Invalid size for memory buffer: " << argv[3] << std::endl;
    return -1;
  }

  // from MB to bytes
  size_t mem_size = n * 1000 * 1000;

  struct stat file_status;
  fstat(fd_input, &file_status);
  unsigned long size = file_status.st_size / sizeof(uint64_t);

  std::cout << "Number of integers: " << size << std::endl
            << "Mem size: " << mem_size << std::endl;

  externalSort(fd_input, size, fd_output, mem_size);
  close(fd_input);
  close(fd_output);

  verify(argv[2]);
  return 0;
}


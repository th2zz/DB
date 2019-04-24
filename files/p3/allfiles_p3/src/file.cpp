/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "file.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <cstdio>
#include <cassert>

#include "exceptions/file_exists_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/file_open_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "file_iterator.h"
#include "page.h"

namespace badgerdb {

File::StreamMap File::open_streams_;
File::CountMap File::open_counts_;

void File::remove(const std::string& filename) {
  if (!exists(filename)) {
    throw FileNotFoundException(filename);
  }
  if (isOpen(filename)) {
    throw FileOpenException(filename);
  }
  std::remove(filename.c_str());
}

bool File::isOpen(const std::string& filename) {
  if (!exists(filename)) {
    return false;
  }
  return open_counts_.find(filename) != open_counts_.end();
}

bool File::exists(const std::string& filename) {
	std::fstream file(filename);
	if(file)
	{
		file.close();
		return true;
	}

	return false;
}

File::~File() {
  close();
}


PageId File::getFirstPageNo() {
  const FileHeader& header = readHeader();
  return header.first_used_page;
}

File::File(const std::string& name, const bool create_new) : filename_(name) {
  openIfNeeded(create_new);

  if (create_new) {
    // File starts with 1 page (the header).
    FileHeader header = {1 /* num_pages */, 0 /* first_used_page */,
                         0 /* num_free_pages */, 0 /* first_free_page */};
    writeHeader(header);
  }
}

void File::openIfNeeded(const bool create_new) {
  if (open_counts_.find(filename_) != open_counts_.end()) {	//exists an entry already
    ++open_counts_[filename_];
    stream_ = open_streams_[filename_];
  } else {
    std::ios_base::openmode mode =
        std::fstream::in | std::fstream::out | std::fstream::binary;
    const bool already_exists = exists(filename_);
    if (create_new) {
      // Error if we try to overwrite an existing file.
      if (already_exists) {
        throw FileExistsException(filename_);
      }
      // New files have to be truncated on open.
      mode = mode | std::fstream::trunc;
    } else {
      // Error if we try to open a file that doesn't exist.
      if (!already_exists) {
        throw FileNotFoundException(filename_);
      }
    }
    stream_.reset(new std::fstream(filename_, mode));
    open_streams_[filename_] = stream_;
    open_counts_[filename_] = 1;
  }
}

void File::close() {
	if(open_counts_[filename_] > 0)
  	--open_counts_[filename_];

  stream_.reset();
	assert(open_counts_[filename_] >= 0);

  if (open_counts_[filename_] == 0) {
    open_streams_.erase(filename_);
    open_counts_.erase(filename_);
  }
}

FileHeader File::readHeader() const {
  FileHeader header;
  stream_->seekg(0 /* pos */, std::ios::beg);
  stream_->read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
  return header;
}

void File::writeHeader(const FileHeader& header) {
  stream_->seekp(0 /* pos */, std::ios::beg);
  stream_->write(reinterpret_cast<const char*>(&header), sizeof(FileHeader));
  stream_->flush();
}





PageFile PageFile::create(const std::string& filename) {
  return PageFile(filename, true /* create_new */);
}

PageFile PageFile::open(const std::string& filename) {
  return PageFile(filename, false /* create_new */);
}

PageFile::PageFile(const std::string& name, const bool create_new)
: File(name, create_new)
{
}

PageFile::~PageFile() {
}

PageFile::PageFile(const PageFile& other)
: File(other.filename_, false /* create_new */)
{
}

PageFile& PageFile::operator=(const PageFile& rhs) {
  // This accounts for self-assignment and assignment of a File object for the
  // same file.
  close();	//close my file and associate me with the new one
  filename_ = rhs.filename_;
  openIfNeeded(false /* create_new */);
  return *this;
}

Page PageFile::allocatePage(PageId &new_page_number) {
  FileHeader header = readHeader();
  Page new_page;
  Page existing_page;
  if (header.num_free_pages > 0) {
    new_page = readPage(header.first_free_page, true /* allow_free */);
    new_page.set_page_number(header.first_free_page);
		new_page_number = new_page.page_number();
    header.first_free_page = new_page.next_page_number();
    --header.num_free_pages;

    if (header.first_used_page == Page::INVALID_NUMBER ||
        header.first_used_page > new_page.page_number()) {
      // Either have no pages used or the head of the used list is a page later
      // than the one we just allocated, so add the new page to the head.
      if (header.first_used_page > new_page.page_number()) {
        new_page.set_next_page_number(header.first_used_page);
      }
      header.first_used_page = new_page.page_number();
    } else {
      // New page is reused from somewhere after the beginning, so we need to
      // find where in the used list to insert it.
      PageId next_page_number = Page::INVALID_NUMBER;
      for (FileIterator iter = begin(); iter != end(); ++iter) {
        next_page_number = (*iter).next_page_number();
        if (next_page_number > new_page.page_number() ||
            next_page_number == Page::INVALID_NUMBER) {
          existing_page = *iter;
          break;
        }
      }
      existing_page.set_next_page_number(new_page.page_number());
      new_page.set_next_page_number(next_page_number);
    }

    assert((header.num_free_pages == 0) ==
           (header.first_free_page == Page::INVALID_NUMBER));
  }
	else
	{
    new_page.set_page_number(header.num_pages);
		new_page_number = new_page.page_number();

    if (header.first_used_page == Page::INVALID_NUMBER)
		{
      header.first_used_page = new_page.page_number();
    }
		else
		{
      // If we have pages allocated, we need to add the new page to the tail
      // of the linked list.
      for (FileIterator iter = begin(); iter != end(); ++iter) {
        if ((*iter).next_page_number() == Page::INVALID_NUMBER) {
          existing_page = *iter;
          break;
        }
      }
      assert(existing_page.isUsed());
      existing_page.set_next_page_number(new_page.page_number());
    }
    ++header.num_pages;
  }
  writePage(new_page_number, new_page.header_, new_page);
  if (existing_page.page_number() != Page::INVALID_NUMBER) {
    // If we updated an existing page by inserting the new page into the
    // used list, we need to write it out.
    writePage(existing_page.page_number(), existing_page.header_, existing_page);
  }
  writeHeader(header);

  return new_page;
}

Page PageFile::readPage(const PageId page_number) const {
  FileHeader header = readHeader();

	if (page_number >= header.num_pages)
	{
		throw InvalidPageException(page_number, filename_);
	}
	return readPage(page_number, false /* allow_free */);
}

Page PageFile::readPage(const PageId page_number, const bool allow_free) const {
  Page page;
  stream_->seekg(pagePosition(page_number), std::ios::beg);
  stream_->read(reinterpret_cast<char*>(&page.header_), sizeof(PageHeader));
  stream_->read(reinterpret_cast<char*>(&page.data_[0]), Page::DATA_SIZE);
  if (!allow_free && !page.isUsed()) {
    throw InvalidPageException(page_number, filename_);
  }

  return page;
}

void PageFile::writePage(const PageId new_page_number, const Page& new_page) {
	PageHeader header = readPageHeader(new_page_number);
	if (header.current_page_number == Page::INVALID_NUMBER)
	{
		// Page has been deleted since it was read.
		throw InvalidPageException(new_page_number, filename_);
	}
	// Page on disk may have had its next page pointer updated since it was read;
	// we don't modify that, but we do keep all the other modifications to the
	// page header.
	const PageId next_page_number = header.next_page_number;
	header = new_page.header_;
	header.next_page_number = next_page_number;
	writePage(new_page_number, header, new_page);
}

void PageFile::deletePage(const PageId page_number) {
  FileHeader header = readHeader();

  Page existing_page = readPage(page_number);
  Page previous_page;
  // If this page is the head of the used list, update the header to point to
  // the next page in line.
  if (page_number == header.first_used_page) {
    header.first_used_page = existing_page.next_page_number();
  } else {
    // Walk the used list so we can update the page that points to this one.
    for (FileIterator iter = begin(); iter != end(); ++iter) {
      previous_page = *iter;
      if (previous_page.next_page_number() == existing_page.page_number()) {
        previous_page.set_next_page_number(existing_page.next_page_number());
        break;
      }
    }
  }
  // Clear the page and add it to the head of the free list.
  existing_page.initialize();
  existing_page.set_next_page_number(header.first_free_page);
  header.first_free_page = page_number;
  ++header.num_free_pages;
  if (previous_page.isUsed()) {
    writePage(previous_page.page_number(), previous_page.header_, previous_page);
  }
  writePage(page_number, existing_page.header_, existing_page);
  writeHeader(header);
}

FileIterator PageFile::begin() {
  const FileHeader& header = readHeader();
  return FileIterator(this, header.first_used_page);
}

FileIterator PageFile::end() {
  return FileIterator(this, Page::INVALID_NUMBER);
}

void PageFile::writePage(const PageId page_number, const PageHeader& header,
                     const Page& new_page) {
  stream_->seekp(pagePosition(page_number), std::ios::beg);
  stream_->write(reinterpret_cast<const char*>(&header), sizeof(PageHeader));
  stream_->write(reinterpret_cast<const char*>(&new_page.data_[0]),
                 Page::DATA_SIZE);
  stream_->flush();
}

PageHeader PageFile::readPageHeader(PageId page_number) const {
  PageHeader header;
  stream_->seekg(pagePosition(page_number), std::ios::beg);
  stream_->read(reinterpret_cast<char*>(&header), sizeof(PageHeader));
  return header;
}




BlobFile BlobFile::create(const std::string& filename) {
  return BlobFile(filename, true /* create_new */);
}

BlobFile BlobFile::open(const std::string& filename) {
  return BlobFile(filename, false /* create_new */);
}

BlobFile::BlobFile(const std::string& name, const bool create_new)
: File(name, create_new) {
}

BlobFile::~BlobFile() {
}

BlobFile::BlobFile(const BlobFile& other)
: File(other.filename_, false /* create_new */)
{
}

BlobFile& BlobFile::operator=(const BlobFile& rhs) {
  // This accounts for self-assignment and assignment of a File object for the
  // same file.
  close();	//close my file and associate me with the new one
  filename_ = rhs.filename_;
  openIfNeeded(false /* create_new */);
  return *this;
}

Page BlobFile::allocatePage(PageId &new_page_number) {
  FileHeader header = readHeader();
	Page new_page;

	new_page_number = header.num_pages;

	if (header.first_used_page == Page::INVALID_NUMBER) {
		header.first_used_page = header.num_pages;
	}

	++header.num_pages;

	writePage(new_page_number, new_page);
	writeHeader(header);

	return new_page;
}

Page BlobFile::readPage(const PageId page_number) const {
	Page page;
	stream_->seekg(pagePosition(page_number), std::ios::beg);
	stream_->read(reinterpret_cast<char*>(&page), Page::SIZE);
	return page;
}

void BlobFile::writePage(const PageId new_page_number, const Page& new_page) {
	stream_->seekp(pagePosition(new_page_number), std::ios::beg);
	stream_->write(reinterpret_cast<const char*>(&new_page), Page::SIZE);
	stream_->flush();
}

//delePage should not be called for a blob_file, not supported
void BlobFile::deletePage(const PageId page_number) {
	throw InvalidPageException(page_number, filename_);
}

}

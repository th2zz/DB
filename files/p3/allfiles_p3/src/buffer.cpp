/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  //Flush out all unwritten pages
  for (std::uint32_t i = 0; i < numBufs; i++) 
  {
  	BufDesc* tmpbuf = &bufDescTable[i];
  	if (tmpbuf->valid == true && tmpbuf->dirty == true)
		{
			tmpbuf->file->writePage(tmpbuf->pageNo, bufPool[i]);
  	}
  }

  delete [] bufDescTable;
  delete [] bufPool;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  // perform first part of clock algorithm to search for 
  // open buffer frame
  // Assumes non-concurrent access to buffer manager
  std::uint32_t numScanned = 0;
  bool found = 0;

  while (numScanned < 2*numBufs)	//Need to scn twice
  {
    // advance the clock
    advanceClock();
    numScanned++;

    // if invalid, use frame
    if (! bufDescTable[clockHand].valid)
    {
      break;
    }

    // is valid, check referenced bit
    if (! bufDescTable[clockHand].refbit)
    {
      // check to see if someone has it pinned
      if (bufDescTable[clockHand].pinCnt == 0)
      {
        // hasn't been referenced and is not pinned, use it
        // remove previous entry from hash table
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        found = true;
        break;
      }
    }
    else
    {
      // has been referenced, clear the bit
      bufStats.accesses++;
      bufDescTable[clockHand].refbit = false;
    }
  }
  
  // check for full buffer pool
  if (!found && numScanned >= 2*numBufs)
  {
    throw BufferExceededException();
  }
  
  // flush any existing changes to disk if necessary
  if (bufDescTable[clockHand].dirty)
  {
    bufStats.diskwrites++;
    //status = bufDescTable[clockHand].file->writePage(bufDescTable[clockHand].pageNo,
    bufDescTable[clockHand].file->writePage(bufDescTable[clockHand].pageNo, bufPool[clockHand]);
  }

	//Reset all the BufDesc entry for the frame before returning the frame
  bufDescTable[clockHand].Clear();

  // return new frame number
  frame = clockHand;
} // end allocBuf

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  // check to see if it is already in the buffer pool
  // std::cout << "readPage called on file.page " << file << "." << pageNo << endl;
  FrameId frameNo = 0;
	try
	{
  	hashTable->lookup(file, pageNo, frameNo);

    // set the referenced bit
    bufDescTable[frameNo].refbit = true;
    bufDescTable[frameNo].pinCnt++;
    page = &bufPool[frameNo];
  }
  catch(HashNotFoundException e) //not in the buffer pool, must allocate a new page
  {
    // alloc a new frame
    allocBuf(frameNo);

    // read the page into the new frame
    bufStats.diskreads++;
    //status = file->readPage(pageNo, &bufPool[frameNo]);
    bufPool[frameNo] = file->readPage(pageNo);

    // set up the entry properly
    bufDescTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    // insert in the hash table
    hashTable->insert(file, pageNo, frameNo);
  }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, 
			     const bool dirty) 
{
  // lookup in hashtable
  FrameId frameNo = 0;
  hashTable->lookup(file, pageNo, frameNo);

  if (dirty == true) bufDescTable[frameNo].dirty = dirty;

  // make sure the page is actually pinned
  if (bufDescTable[frameNo].pinCnt == 0)
  {
  	throw PageNotPinnedException(file->filename(), pageNo, frameNo);
  }
  else bufDescTable[frameNo].pinCnt--;
}

void BufMgr::flushFile(const File* file) 
{
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	BufDesc* tmpbuf = &(bufDescTable[i]);
  	if(tmpbuf->valid == true && tmpbuf->file == file)
		{
	    if (tmpbuf->pinCnt > 0)
  			throw PagePinnedException(file->filename(), tmpbuf->pageNo, tmpbuf->frameNo);

	    if (tmpbuf->dirty == true)
			{
				//if ((status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]))) != OK)
				tmpbuf->file->writePage(tmpbuf->pageNo, bufPool[i]);
				tmpbuf->dirty = false;
    	}

    	hashTable->remove(file,tmpbuf->pageNo);
    	tmpbuf->Clear();
  	}
		else if (tmpbuf->valid == false && tmpbuf->file == file)
  		throw BadBufferException(tmpbuf->frameNo, tmpbuf->dirty, tmpbuf->valid, tmpbuf->refbit);
  }
}

void BufMgr::disposePage(File* file, const PageId pageNo) 
{
	//Deallocate from file altogether
  //See if it is in the buffer pool
  FrameId frameNo = 0;
  hashTable->lookup(file, pageNo, frameNo);

	// clear the page
	bufDescTable[frameNo].Clear();

	hashTable->remove(file, pageNo);

  // deallocate it in the file	
  file->deletePage(pageNo);
}


void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  FrameId frameNo;

  // alloc a new frame
  allocBuf(frameNo);

  // allocate a new page in the file
	//std::cerr << "buffer data size:" << bufPool[frameNo].data_.length() << "\n";
  bufPool[frameNo] = file->allocatePage(pageNo);
  page = &bufPool[frameNo];

  // set up the entry properly
  bufDescTable[frameNo].Set(file, pageNo);

  // insert in the hash table
  hashTable->insert(file, pageNo, frameNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}

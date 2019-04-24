/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "filescan.h"
#include "exceptions/end_of_file_exception.h"

namespace badgerdb { 

FileScan::FileScan(const std::string &name, BufMgr *bufferMgr)
{
  file = new PageFile(name, false);	//dont create new file
	bufMgr = bufferMgr;
	curDirtyFlag = false;
  curPage = NULL;
	filePageIter = file->begin();
}

FileScan::~FileScan()
{
  // generally must unpin last page of the scan
  if (curPage != NULL)
  {
    bufMgr->unPinPage(file, (*filePageIter).page_number(), curDirtyFlag);
    curPage = NULL;
		curDirtyFlag = false;
    filePageIter = file->begin();
  }
  bufMgr->flushFile(file);
  delete file;
}

void FileScan::scanNext(RecordId& outRid)
{
  std::string rec;

  if (filePageIter == file->end())
	{
		throw EndOfFileException();
	}

  // special case of the first record of the first page of the file
  if (curPage == NULL)
  {
    // need to get the first page of the file
		filePageIter = file->begin();
    if(filePageIter == file->end())
		{
			throw EndOfFileException();
		}
	 
		// read the first page of the file
    bufMgr->readPage(file, (*filePageIter).page_number(), curPage); 
		curDirtyFlag = false;

		// get the first record off the page
    pageRecordIter = curPage->begin(); 

		if(pageRecordIter != curPage->end()) 
		{
		  // get pointer to record
		  rec = *pageRecordIter;

			outRid = pageRecordIter.getCurrentRecord();
			return;
		}
  }

	// Loop, looking for a record that satisfied the predicate.
	// First try and get the next record off the current page
	pageRecordIter++;

  while (pageRecordIter == curPage->end())
  {
    // unpin the current page
    bufMgr->unPinPage(file, (*filePageIter).page_number(), curDirtyFlag);
    curPage = NULL;
    curDirtyFlag = false;

    filePageIter++;
    if (filePageIter == file->end())
    {
      curPage = NULL;
			throw EndOfFileException();
    }

    // read the next page of the file
    bufMgr->readPage(file, (*filePageIter).page_number(), curPage);

    // get the first record off the page
    pageRecordIter = curPage->begin(); 
  }

  // curRec points at a valid record
  // see if the record satisfies the scan's predicate 
  // get a pointer to the record
  rec = *pageRecordIter;

	// return rid of the record
	outRid = pageRecordIter.getCurrentRecord();
	return;
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 
std::string FileScan::getRecord()
{
  return *pageRecordIter;
}

// mark current page of scan dirty
void FileScan::markDirty()
{
  curDirtyFlag = true;
}

}

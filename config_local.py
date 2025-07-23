import os

CoreServerURL = "http://localhost:8000"

ImageRootDirectory = "/home/seele/Desktop/WorkSpace/PipelineWorker"
ImageTransferTemp = os.path.join(ImageRootDirectory, "transfer_temp")
ImageProcessedFilesArchive = os.path.join(ImageRootDirectory, "Processed_Files_Archive")

HNDBRootDirectory = "/home/seele/Desktop/WorkSpace/PipelineWorker/HNDB"
SamplePreparationDirectory = os.path.join(HNDBRootDirectory, "HNDB_files", "SamplePreparation")
Cell_ImageDirectory = os.path.join(HNDBRootDirectory, "Cell_Image")
ImageArchiveDirectory = os.path.join(HNDBRootDirectory, "PTRSB_DB", "ImageArchive")
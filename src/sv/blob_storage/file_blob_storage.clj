(ns sv.blob-storage.file-blob-storage
  (:require [sv.blob-storage :as bs]
            [clojure.java.io :as io]))

(defn- uuid []
  (java.util.UUID/randomUUID))

(deftype FileBlobStorage [^java.io.File dir]
  bs/BlobStorage
  (store [this in]
    (let [id (uuid)
          file (io/file dir (str id))]
      (try
        (io/copy
         in
         file)
        (finally
          (.close in)))
      id))
  (retrieve [this id]
    (let [file (io/file dir (str id))]
      (when (.exists file)
        (io/input-stream
         file))))
  (exists [this id]
    (let [file (io/file dir (str id))]
      (.exists file)))
  (delete [this id]
    (.delete (io/file dir (str id)))))

(defn file-blob-storage [^java.io.File dir]
  (assert (.isDirectory dir))
  (->FileBlobStorage dir))

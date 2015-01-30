/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <image/image_util.hpp>
#include <image/image_util_impl.hpp>
#include <sframe/sframe_iterators.hpp>
#include <sframe/sframe.hpp>
#include <unity/query_process/lazy_sarray.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>
#include <unity/lib/toolkit_function_macros.hpp>
#include <fileio/sanitize_url.hpp>


namespace graphlab{

namespace image_util{

/**
* Return the head of passed sarray, but cast to string. Used for printing on python side. 
*/

  std::shared_ptr<unity_sarray> _head_str(std::shared_ptr<unity_sarray> image_sarray, size_t num_rows){
    log_func_entry();
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    sarray<flexible_type> out_sarray;
    out_sarray.open_for_write(1);
    out_sarray.set_type(flex_type_enum::STRING);
    size_t nrows = std::min<size_t>(num_rows, image_sarray->size());
    if (image_sarray->get_lazy_sarray()) {
      graphlab::copy<flexible_type>(image_sarray->get_lazy_sarray(), out_sarray.get_output_iterator(0), nrows);
    } 
    out_sarray.close();
    ret->construct_from_sarray(std::make_shared<sarray<flexible_type>>(out_sarray));
    return ret;
  }


  /**
  * Return flex_vec flexible type that is sum of all images with data in vector form. 
  */

  flexible_type sum(std::shared_ptr<unity_sarray> unity_data){
    log_func_entry();

    if (unity_data->size() > 0){
      bool failure = false;
      size_t reference_size;
      size_t failure_size; 
      auto reductionfn =
        [&failure, &reference_size, &failure_size](const flexible_type& in, std::pair<bool, flexible_type>& sum)->bool {
          if (in.get_type() != flex_type_enum::UNDEFINED) {
            flexible_type tmp_img = in;
            image_util_detail::decode_image_impl(tmp_img.mutable_get<flex_image>());
            flexible_type f(flex_type_enum::VECTOR);
            f.soft_assign(tmp_img);
            if (sum.first == false) {
              // initial val
              sum.first = true;
              sum.second = f;
            } else if (sum.second.size() == f.size()) {
              // accumulation
              sum.second += f;
            } else {
              // length mismatch. fail
              failure = true;
              reference_size = sum.second.size();
              failure_size = f.size();
              return false;
            }
          }
          return true;
        };

      auto combinefn =
        [&failure, &reference_size, &failure_size](const std::pair<bool, flexible_type>& f, std::pair<bool, flexible_type>& sum)->bool {
          if (sum.first == false) {
            // initial state
            sum = f;
          } else if (f.first == false) {
            // there is no f to add.
            return true;
          } else if (sum.second.size() == f.second.size()) {
            // accumulation
            sum.second += f.second;
          } else {
            // length mismatch
            failure = true;
            reference_size = sum.second.size();
            failure_size = f.second.size();
            return false;
          }
          return true;
        };
      std::pair<bool, flexible_type> start_val{false, flex_vec()};
      std::pair<bool, flexible_type> sum_val =
        graphlab::reduce<std::pair<bool, flexible_type> >(unity_data->get_lazy_sarray(), reductionfn, combinefn , start_val);

      if(failure){
        std::string error = std::string("Cannot perform sum or average over images of different sizes. ")
                          + std::string("Found images of total size (ie. width * height * channels) ")
                          + std::string(" of both ") + std::to_string(reference_size) + std::string( "and ") + std::to_string(failure_size) 
                          + std::string(". Please use graplab.image_analysis.resize() to make images a uniform") 
                          + std::string(" size.");
        log_and_throw(error);
      }
      return sum_val.second;
    }

    log_and_throw("Input image sarray is empty");
    __builtin_unreachable();
  }


/**
 * Construct an image of mean pixel values from a pointer to a unity_sarray.
 */

flexible_type generate_mean(std::shared_ptr<unity_sarray> unity_data) {
  log_func_entry();

  
  std::vector<flexible_type> sample_img = unity_data->_head(1);
  flex_image meta_img;
  meta_img = sample_img[0];
  size_t channels = meta_img.m_channels;
  size_t height = meta_img.m_height;
  size_t width = meta_img.m_width;
  size_t image_size = width * height * channels;
  size_t num_images = unity_data->size(); 

  //Perform sum
  flexible_type mean = sum(unity_data);
  
  //Divide for mean images.
  mean /= num_images;

  flexible_type ret;
  flex_image img;
  char* data_bytes = new char[image_size];
  for(size_t i = 0; i < image_size; ++i){
    data_bytes[i] = static_cast<unsigned char>(mean[i]);
  } 
  img.m_image_data_size = image_size;
  img.m_channels = channels;
  img.m_height = height;
  img.m_width = width;
  img.m_image_data.reset(data_bytes);
  img.m_version = IMAGE_TYPE_CURRENT_VERSION;
  img.m_format = Format::RAW_ARRAY;
  ret = img;
  return ret;
}


/**
 * Construct a single image from url, and format hint.
 */
flexible_type load_image(const std::string& url, const std::string format) {
  flexible_type ret(flex_type_enum::IMAGE);
  ret = read_image(url, format);
  return ret;
};

size_t load_images_impl(std::vector<std::string>& all_files,
                        sarray<flexible_type>::iterator& image_iter,
                        sarray<flexible_type>::iterator& path_iter,
                        const std::string& format,
                        bool with_path, bool ignore_failure, size_t thread_id) {
  timer mytimer;

  atomic<size_t> cnt = 0;
  double previous_time = 0;
  int previous_cnt = 0;
  bool cancel = false;

  auto iter = all_files.begin();
  auto end = all_files.end();
  while(iter < end && !cancel) {
    flexible_type img(flex_type_enum::IMAGE);
    // read a single image
    try {
      img = read_image(*iter, format);
      *image_iter = img;
      ++image_iter;
      if (with_path) {
        *path_iter = *iter;
        ++path_iter;
      }
      ++cnt;
    } catch (std::string error) {
      logprogress_stream << error << "\t" << " file: " << sanitize_url(*iter) << std::endl;
      if (!ignore_failure)
        throw;
    } catch (const char* error) {
      logprogress_stream << error << "\t" << " file: " << sanitize_url(*iter) << std::endl;
      if (!ignore_failure)
        throw;
    } catch (...) {
      logprogress_stream << "Unknown error reading image \t file: " << sanitize_url(*iter) << std::endl;
      if (!ignore_failure)
        throw;
    }
    ++iter;
    // output progress 
    if (thread_id == 0) {
      double current_time = mytimer.current_time();
      size_t current_cnt = cnt;
      if (current_time - previous_time > 5) {
        logprogress_stream << "Read " << current_cnt << " images in " << current_time << " secs\t"
          << "speed: " << double(current_cnt - previous_cnt) / (current_time - previous_time)
          << " file/sec" << std::endl;
        previous_time = current_time;
        previous_cnt = current_cnt;
      }
    }
    // check for user interrupt ctrl-C
    if (cppipc::must_cancel()) {
      cancel = true;
    }
  } // end of while

  if (cancel)  {
    log_and_throw("Cancelled by user");
  }
  return cnt;
}

std::vector<std::string> get_directory_files(std::string url, bool recursive) {
  typedef std::vector<std::pair<std::string, fileio::file_status>> path_status_vec_t;
  path_status_vec_t path_status_vec = fileio::get_directory_listing(url);
  std::vector<std::string> ret;
  for (const auto& path_status : path_status_vec) {
    if (path_status.first[0] != '.') {
      if (recursive && path_status.second == fileio::file_status::DIRECTORY) {
        auto tmp = get_directory_files(path_status.first, recursive);
        ret.insert(ret.end(), tmp.begin(), tmp.end());
      } else if (path_status.second == fileio::file_status::REGULAR_FILE){
        ret.push_back(path_status.first);
      }
    }
  }
  return ret;
}

/**
 * Construct an sframe of flex_images, with url pointing to directory where images reside. 
 */
std::shared_ptr<unity_sframe> load_images(std::string url, std::string format, bool with_path, bool recursive,
                                          bool ignore_failure, bool random_order) {
    log_func_entry();
    std::vector<std::string> all_files = get_directory_files(url, recursive);

    std::vector<std::string> column_names;
    std::vector<flex_type_enum> column_types;

    sframe image_sframe;
    auto path_sarray = std::make_shared<sarray<flexible_type>>();
    auto image_sarray = std::make_shared<sarray<flexible_type>>();

    // Parallel read dioes not seems to help, and it slow IO down when there is only one disk.
    // We can expose this option in the future for parallel disk IO or RAID.
    // size_t num_threads = thread_pool::get_instance().size();
    size_t num_threads = 1;
    column_names = {"path","image"};
    path_sarray->open_for_write(num_threads + 1); // open one more segment for appending recursive results
    image_sarray->open_for_write(num_threads + 1); // ditto
    path_sarray->set_type(flex_type_enum::STRING);
    image_sarray->set_type(flex_type_enum::IMAGE);

    if (random_order) {
      std::random_shuffle(all_files.begin(), all_files.end());
    } else {
      std::sort(all_files.begin(), all_files.end());
    }

    size_t files_per_thread = all_files.size() / num_threads;
    parallel_for(0, num_threads, [&](size_t thread_id) {
      auto path_iter = path_sarray->get_output_iterator(thread_id);
      auto image_iter = image_sarray->get_output_iterator(thread_id);

      size_t begin = files_per_thread * thread_id;
      size_t end = (thread_id + 1) == num_threads ? all_files.size() : begin + files_per_thread;

      std::vector<std::string> subset(all_files.begin() + begin, all_files.begin() + end);
      load_images_impl(subset, image_iter, path_iter, format, with_path, ignore_failure, thread_id);
    });

    image_sarray->close();
    path_sarray->close();

    if (with_path){
      std::vector<std::shared_ptr<sarray<flexible_type> > > sframe_columns {path_sarray, image_sarray};
      image_sframe = sframe(sframe_columns, {"path","image"});
    } else {
      std::vector<std::shared_ptr<sarray<flexible_type> > > sframe_columns {image_sarray};
      image_sframe = sframe(sframe_columns, {"image"});
    }

    std::shared_ptr<unity_sframe> image_unity_sframe(new unity_sframe());
    image_unity_sframe->construct_from_sframe(image_sframe);

    return image_unity_sframe;
}


void decode_image_inplace(image_type& image) {
  image_util_detail::decode_image_impl(image); 
}

/**
 * Decode the image into raw pixels
 */
flexible_type decode_image(flexible_type image) {
  flex_image& tmp = image.mutable_get<flex_image>();
  image_util_detail::decode_image_impl(tmp);
  return flexible_type(tmp);
};

/**
 * Decode an sarray of flex_images into raw pixels
 */
std::shared_ptr<unity_sarray> decode_image_sarray(std::shared_ptr<unity_sarray> image_sarray) {
  auto transform_operator= std::make_shared<le_transform<flexible_type>>(
    image_sarray->get_query_tree(),
    [=](const flexible_type& f)->flexible_type {
      return decode_image(f);
    }, flex_type_enum::IMAGE);
  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(transform_operator,
                                                 false,
                                                 flex_type_enum::IMAGE);
  return ret_unity_sarray;
};

/**
 * Reisze an sarray of flex_images with the new size.
 */
flexible_type resize_image(flexible_type image, size_t resized_width, size_t resized_height, size_t resized_channels) {
  if (image.get_type() != flex_type_enum::IMAGE){
    std::string error = "Cannot resize non-image type";
    log_and_throw(error);
  }
  flex_image src_img = image.mutable_get<flex_image>();
  image_util_detail::decode_image_impl(src_img);
  flex_image dst_img;
  char* resized_data;
  image_util_detail::resize_image_impl((const char*)src_img.get_image_data(),
      src_img.m_width, src_img.m_height, src_img.m_channels, resized_width,
      resized_height, resized_channels, &resized_data);
  dst_img.m_width =resized_width;
  dst_img.m_height = resized_height;
  dst_img.m_channels = resized_channels;
  dst_img.m_format = src_img.m_format;
  dst_img.m_image_data_size = resized_height * resized_width * resized_channels;
  dst_img.m_image_data.reset(resized_data);
  image_util_detail::encode_image_impl(dst_img);
  return dst_img;
};


/**
 * Resize an sarray of flex_image with the new size.
 */
std::shared_ptr<unity_sarray> resize_image_sarray(std::shared_ptr<unity_sarray> image_sarray, size_t resized_width, size_t resized_height, size_t resized_channels) {
  log_func_entry();
  auto transform_operator= std::make_shared<le_transform<flexible_type>>(
    image_sarray->get_query_tree(),
    [=](const flexible_type& f)->flexible_type {
      return flexible_type(resize_image(f, resized_width, resized_height, resized_channels));
    }, flex_type_enum::IMAGE);
  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(transform_operator,
                                                 false,
                                                 flex_type_enum::IMAGE);
  return ret_unity_sarray;
};

/**
 * Convert sarray of image data to sarray of vector
 */
std::shared_ptr<unity_sarray> image_sarray_to_vector_sarray(std::shared_ptr<unity_sarray> image_sarray, bool undefined_on_failure) {
  // decoded_image_sarray
  log_func_entry();

  // transform the array with type casting
  auto transform_operator= std::make_shared<le_transform<flexible_type>>(
    image_sarray->get_query_tree(),
    [=](const flexible_type& f)->flexible_type {
      flexible_type ret(flex_type_enum::VECTOR);
      flex_image tmp_img = f;
      image_util_detail::decode_image_impl(tmp_img); 
      try {
        ret = tmp_img;
      } catch (...) {
        if (undefined_on_failure) {
          ret = FLEX_UNDEFINED;
        } else {
          throw;
        }
      }
      return ret;
    }, flex_type_enum::VECTOR);

  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(transform_operator,
                                                 false,
                                                 flex_type_enum::VECTOR);
  return ret_unity_sarray;
};

/**
 * Convert sarray of vector to sarray of image 
 */
std::shared_ptr<unity_sarray> vector_sarray_to_image_sarray(std::shared_ptr<unity_sarray> image_sarray,
                                                            size_t width, size_t height, size_t channels,
                                                            bool undefined_on_failure) {
  log_func_entry();
  size_t expected_array_size = height * width * channels;
  auto transformfn = [height,width,channels,expected_array_size,undefined_on_failure](const flex_vec& vec)->flexible_type {
    try {
      if (expected_array_size != vec.size()) { 
        logprogress_stream << "Dimensions do not match vec size" << std::endl;
        log_and_throw("Dimensions do not match vec size");
      }
      flexible_type ret;
      flex_image img;
      size_t data_size = vec.size();
      char* data = new char[data_size];
      for (size_t i = 0; i < data_size; ++i){
        data[i] = static_cast<char>(vec[i]);
      }
      img.m_image_data_size = data_size;
      img.m_image_data.reset(data);
      img.m_height = height;
      img.m_width = width; 
      img.m_channels = channels;
      img.m_format = Format::RAW_ARRAY;
      img.m_version = 0;
      ret = img;
      return ret;
    } catch (...) {
      if (undefined_on_failure) {
        return FLEX_UNDEFINED;
      } else {
        throw;
      }
    }
  };
  auto transform_operator= std::make_shared<le_transform<flexible_type>>(
    image_sarray->get_query_tree(),
    transformfn,flex_type_enum::IMAGE);

  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(transform_operator,
                                                 false,
                                                 flex_type_enum::IMAGE);
  return ret_unity_sarray;
};



BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(_head_str, "image_sarray", "num_rows")
REGISTER_FUNCTION(load_image, "url", "format")
REGISTER_FUNCTION(load_images, "url", "format", "with_path", "recursive", "ignore_failure", "random_order")
REGISTER_FUNCTION(decode_image, "image")
REGISTER_FUNCTION(decode_image_sarray, "image_sarray")
REGISTER_FUNCTION(resize_image, "image",  "resized_width", "resized_height", "resized_channels")
REGISTER_FUNCTION(resize_image_sarray, "image_sarray",  "resized_width", "resized_height", "resized_channels")
REGISTER_FUNCTION(vector_sarray_to_image_sarray, "sarray",  "width", "height", "channels", "undefined_on_failure")
REGISTER_FUNCTION(generate_mean, "unity_data")
END_FUNCTION_REGISTRATION



} //namespace image_util 
} //namespace graphlab

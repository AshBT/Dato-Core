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
#include <logger/logger.hpp>
#include <fileio/general_fstream.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/sframe_constants.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp> 
#include <boost/iostreams/device/file.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/python.hpp>
#include <lambda/pyflexible_type.hpp>

using namespace graphlab;
namespace python = boost::python;

constexpr size_t NUM_SEGMENTS = 1;

inline void utf8_decode(const std::string& val, 
                   std::string& output, size_t& output_len) {
  if (output.size() < val.size()) {
    output.resize(val.size());
  }
  char* cur_out = &(output[0]);
  // loop through the input string
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    if((c & 0x80) == 0) { 
      (*cur_out++) = c; 
    } else { 
      if((c & 0xC2) != 0xC2) {
       log_and_throw("utf8 format is wrong, 110xxxxx is required " + std::to_string(int(c)) + " is given"); 
      }
      char outChar = (c << 6);
      if(i < val.size() - 1) { 
        outChar = (outChar | val[i+1]);
        i++;
      } else { 
        log_and_throw("utf8 format is wrong, 10xxxxxx is required");
      }
      (*cur_out++) = outChar;
    }  
 }
 output_len = cur_out - &(output[0]);
}

std::string handle_pyerror() {
    using namespace boost::python;
    using namespace boost;

    PyObject *exc,*val,*tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc,&val,&tb);
    handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb)); 
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc,hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval,htb);
    }
    formatted = str("\n").join(formatted_list);
    return extract<std::string>(formatted);
}

int main(int argc, char **argv) {
     
try {
    Py_Initialize(); 
    
    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <output directory> <batch-or-pickle>" <<  std::endl;
        return -1;
    }
    // input to this binary is either a batch of serialized objects or a single pickle object. 
    // argv[2] argument is to distinguish these two modes. 

    std::string mode = std::string(argv[2]);
    std::vector<std::string> column_names;
    std::vector<flex_type_enum> column_types;
    std::vector<flexible_type> tokens;
    std::string first_line;
    flexible_type first_row;
    flexible_type first_batch;

    boost::iostreams::filtering_istream fin;
    fin.push(std::cin);  

    if (!fin.good()) {
        log_and_throw("Fail reading from standard input");
    }
    if(!std::getline(fin,first_line)) { 
      return 0;
    }
    python::object pickle = python::import("cPickle");
    python::object base64 = python::import("base64");
    
    // it looks like this library is not working with pickled floating variable. TODO: WHY ?
    // We switch to boost.python.base64.  
    //std::string decoded = base64_decode(first_line); 
    //python::object decoded_obj = python::object(python::handle<>(PyString_FromString(decoded.c_str())))
    //python::object unpickle_obj = python::object(pickle.attr("loads")(decoded_obj));
    python::object first_line_obj = python::object(python::handle<>(PyString_FromString(first_line.c_str())));   
    python::object decoded_obj = python::object(base64.attr("b64decode")(first_line_obj));
    python::object unpickle_obj = python::object(pickle.attr("loads")(decoded_obj));
    first_batch = lambda::PyObject_AsFlex(unpickle_obj);
    if(mode == "batch") { 
      if(first_batch.get_type() == flex_type_enum::LIST) { 
        first_row = first_batch.get<flex_list>()[0];
      } else {
        first_row.reset(flex_type_enum::FLOAT);
      }
    } else { 
      first_row = first_batch;
    }
    // infer the schema for the sframe from the first row TODO: perhaps we should do it from the first n rows
    size_t ncols = 1; 
    if(first_row.get_type() == flex_type_enum::DICT) { 
      flex_dict dict = first_row.get<flex_dict>();
      ncols = dict.size();
      column_names.resize(ncols);
      column_types.resize(ncols);
      tokens = std::vector<flexible_type>(ncols,flex_string());

      for (size_t i = 0;i < ncols; ++i) {
        column_names[i] = dict[i].first.get<flex_string>();
        column_types[i] = dict[i].second.get_type();
        if(column_types[i] == flex_type_enum::UNDEFINED) {
          column_types[i] = flex_type_enum::STRING;
        }
        tokens[i].reset(column_types[i]);
      }
    } else if(first_row.get_type() == flex_type_enum::LIST) { 
      flex_list rec = first_row.get<flex_list>();
      ncols = rec.size();
      column_names.resize(ncols);
      column_types.resize(ncols);
      tokens = std::vector<flexible_type>(ncols,flex_string());
      for(size_t i = 0;i < ncols; ++i) {
        column_names[i] = "X"+ std::to_string(i + 1);;
        column_types[i] = rec[i].get_type();
        if(column_types[i] == flex_type_enum::UNDEFINED) {
          column_types[i] = flex_type_enum::STRING;
        }
        tokens[i].reset(column_types[i]);
      }
    } else if(first_row.get_type() == flex_type_enum::VECTOR) { 
      flex_vec vec = first_row.get<flex_vec>();
      ncols = vec.size();
      column_names.resize(ncols);
      column_types.resize(ncols);
      tokens = std::vector<flexible_type>(ncols,flex_string());
      for(size_t i = 0;i < ncols; ++i) {
        column_names[i] = "X"+ std::to_string(i + 1);;
        column_types[i] = flex_type_enum::FLOAT;
        tokens[i].reset(column_types[i]);
      }
    } else{ 
      column_names.resize(ncols); 
      column_types.resize(ncols);
      column_types[0] = first_row.get_type();
      if(column_types[0] == flex_type_enum::UNDEFINED) {
       column_types[0] = flex_type_enum::STRING;
      }
      column_names[0] = "X1";
      tokens = std::vector<flexible_type>(ncols,flex_string());
      tokens[0].reset(column_types[0]);
    }  
    // Create the SFrame to write to
    //TODO: Repeat this until done
    boost::uuids::uuid file_prefix = boost::uuids::random_generator()();
    std::stringstream ss;
    // TODO: Join paths correctly
    ss << argv[1] << "/" << file_prefix << ".frame_idx";
    sframe frame;
    frame.open_for_write(column_names, column_types, "", 1, false);

    for(size_t sidx = 0; sidx < NUM_SEGMENTS; ++sidx) {
        auto it_out = frame.get_output_iterator(sidx);
        flexible_type row;
        std::vector<flexible_type> batch;
        batch.resize(1);
        if(first_batch.get_type() == flex_type_enum::VECTOR && mode == "batch") { 
          flex_vec vect = first_batch.get<flex_vec>();
          for(size_t index = 0 ; index < vect.size() ; index++) { 
            tokens[0] = vect[index];
            *it_out = tokens;
            ++it_out;
          }
        } 
        else { 
          if(mode == "pickle") {
            batch[0] = first_batch;
          } else{
            batch = std::move(first_batch.get<flex_list>());
          }  
          for(size_t index = 0 ; index < batch.size() ; index++) {  
            row = batch[index]; 
            ncols = 1;
          
            if(row.get_type() == flex_type_enum::DICT) { 
              flex_dict dict = row.get<flex_dict>();
              ncols = dict.size();
              for (size_t i = 0;i < ncols; ++i) {
                tokens[i] = dict[i].second;
              }
            } else if (row.get_type() == flex_type_enum::VECTOR) { 
              flex_vec vect = row.get<flex_vec>();
              ncols = vect.size();
 
              for (size_t i = 0;i < ncols; ++i) {
                tokens[i] = vect[i];
              }
            } else if (row.get_type() == flex_type_enum::LIST) { 
      
              flex_list rec = row.get<flex_list>();
              ncols = rec.size();
 
              for (size_t i = 0;i < ncols; ++i) {
                tokens[i] = rec[i];
              }    
            } else { 
              tokens[0] = row;
            }    
            
            *it_out = tokens;
            ++it_out;
          }
        }
    for (std::string curVal; std::getline(fin, curVal); ) {    
          
      python::object curVal_obj = python::object(python::handle<>(PyString_FromString(curVal.c_str())));
      python::object decoded_obj = python::object(base64.attr("b64decode")(curVal_obj));
      python::object unpickle_obj = python::object(pickle.attr("loads")(decoded_obj));
      flexible_type val = lambda::PyObject_AsFlex(unpickle_obj);
            
      if(val.get_type() == flex_type_enum::VECTOR && mode == "batch") { 
        flex_vec vect = val.get<flex_vec>();
        for(size_t index = 0 ; index < vect.size() ; index++) { 
          tokens[0] = vect[index];
          *it_out = tokens;
          ++it_out;
        }
      } 
      else { 
        
        if(mode == "pickle") { 
          batch[0] = val;
        } else {
          batch = val.get<flex_list>();
        }
  
        for(size_t index = 0 ; index < batch.size() ; index++) {  
          row = batch[index]; 
          ncols = 1;
       
          if(row.get_type() == flex_type_enum::DICT) {
  
            flex_dict dict = row.get<flex_dict>();
            ncols = dict.size();
            for (size_t i = 0;i < ncols; ++i) {
              tokens[i] = dict[i].second;
            }

          } else if (row.get_type() == flex_type_enum::VECTOR) { 
            flex_vec vect = row.get<flex_vec>();
            ncols = vect.size();
   
            for (size_t i = 0;i < ncols; ++i) {
              tokens[i] = vect[i];
            }
          } else if (row.get_type() == flex_type_enum::LIST) { 
     
            flex_list rec = row.get<flex_list>();
            ncols = rec.size();
   
            for (size_t i = 0;i < ncols; ++i) {
              tokens[i] = rec[i];
            }       
          } else { 
            tokens[0] = row;
          }    
        
          *it_out = tokens;
          ++it_out;
        }
      }  
    }
  }

  if(frame.is_opened_for_write()) {
    frame.close();
  }
  std::string index_str = ss.str();
  std::cout << index_str << std::endl;
  frame.save(index_str);
  //fin.close();

  } catch (python::error_already_set const& e) {
    if (PyErr_Occurred()) {
      std::string msg = handle_pyerror(); 
      std::cerr << "GRAPHLAB PYTHON-ERROR: " << msg << std::endl;
      throw(msg);
    }
    //bool py_exception = true;
    python::handle_exception();
    PyErr_Clear();
  }
  return 0;
   
}

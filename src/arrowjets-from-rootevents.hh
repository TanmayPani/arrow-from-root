#pragma once

#include <ROOT/RDataFrame.hxx>

#include <arrow/api.h>
#include <arrow/result.h>

#include <vector>
#include <string>


#ifdef _WIN32
  #define ARROWJETS_FROM_ROOTEVENTS_EXPORT __declspec(dllexport)
#else
  #define ARROWJETS_FROM_ROOTEVENTS_EXPORT
#endif

ARROWJETS_FROM_ROOTEVENTS_EXPORT arrow::Status arrowjets_from_rootevents(ROOT::RDF::RNode, const std::string&, unsigned int);
ARROWJETS_FROM_ROOTEVENTS_EXPORT arrow::Status arrowjets_from_rootevents(const std::string& , const std::vector<std::string>& , const std::string& , unsigned int);
ARROWJETS_FROM_ROOTEVENTS_EXPORT arrow::Status arrowjets_from_rootevents(const std::string& , const std::string& , const std::string& , unsigned int);
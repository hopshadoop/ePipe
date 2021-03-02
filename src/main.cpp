/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

#include "Notifier.h"
#include <boost/program_options.hpp>
#include "Version.h"
#include "Reindexer.h"
#include "FeaturestoreReindexer.h"

namespace po = boost::program_options;

int main(int argc, char** argv) {
  
  try {
    std::string connection_string;
    std::string database_name = "hops";
    std::string meta_database_name = "hopsworks";
    std::string hive_meta_database_name = "metastore";

    int poll_maxTimeToWait = 2000;
    std::string elastic_addr = "localhost:9200";
    LogSeverityLevel log_level = LogSeverityLevel::info;

    TableUnitConf mutations_tu = TableUnitConf();
    TableUnitConf schamebased_tu = TableUnitConf();
    TableUnitConf provenance_tu = TableUnitConf();

    bool hopsworks = true;
    std::string elastic_index = "projects";
    int elastic_batch_size = 5000;
    int elastic_issue_time = 5000;

    std::string elastic_featurestore_index = "featurestore";
    std::string elastic_app_provenance_index = "appprovenance";
    std::string elastic_ml_index = "ml";

    int lru_cap = DEFAULT_MAX_CAPACITY;
    int prov_file_lru_cap = DEFAULT_MAX_CAPACITY;
    int prov_core_lru_cap = 100;
    bool recovery = true;
    bool stats = true;

    Barrier barrier = EPOCH;

    bool reindex = false;
    std::string reindex_of = "all";

    bool hiveCleaner = true;

    std::string metricsServer = "0.0.0.0:9191";

    bool sslEnabled = false;
    std::string caPath = "";
    std::string username = "";
    std::string password = "";

    int log_rotation_size = 64 * 1024 * 1024;
    std::string log_dir = "/tmp/epipe/logs";
    int log_max_files = 10;

    po::options_description generalOptions("General");
    generalOptions.add_options()
        ("help,h", "Help screen")
        ("desc,d", "Description of the allowed configuration parameters")
        ("version,v", "ePipe version")
        ("config,c", po::value<std::string>(), "Config file");


    po::options_description fileOptions("Allowed options");
    fileOptions.add_options()
        ("connection", po::value<std::string>(&connection_string),
         "connection std::string/ ndb host")
        ("database",
         po::value<std::string>(&database_name)->default_value(database_name),
         "database name.")
        ("meta_database", po::value<std::string>(&meta_database_name)->default_value(
            meta_database_name), "database name for metadata")
        ("hive_meta_database",
         po::value<std::string>(&hive_meta_database_name)->default_value(
             hive_meta_database_name), "database name for hive metadata")
        ("poll_maxTimeToWait",
         po::value<int>(&poll_maxTimeToWait)->default_value(poll_maxTimeToWait),
         "max time to wait in miliseconds while waiting for events in pollEvents")
        ("fs_mutations_tu",
         po::value<std::vector<int> >()->default_value(mutations_tu.getVector(),
                                                  mutations_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
        ("schamebased_tu",
         po::value<std::vector<int> >()->default_value(schamebased_tu.getVector(),
                                                  schamebased_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
        ("provenance_tu",
         po::value<std::vector<int> >()->default_value(provenance_tu.getVector(),
                                                  provenance_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
         ("elastic_addr",
         po::value<std::string>(&elastic_addr)->default_value(elastic_addr),
         "ip and port of the elasticsearch server")
        ("hopsworks", po::value<bool>(&hopsworks)->default_value(hopsworks),
         "enable or disable hopsworks")
        ("hivecleaner",
         po::value<bool>(&hiveCleaner)->default_value(hiveCleaner),
         "enable or disable hiveCleaner")
        ("index", po::value<std::string>(&elastic_index)->default_value(elastic_index), "Elastic index to add the data to.")
        ("featurestore_index", po::value<std::string>(&elastic_featurestore_index)->default_value(elastic_featurestore_index), "Elastic featurestore index.")
        ("app_provenance_index", po::value<std::string>(&elastic_app_provenance_index)->default_value(elastic_app_provenance_index), "Elastic index to add the app provenance data to.")
        ("ml_provenance_index", po::value<std::string>(&elastic_ml_index)->default_value(elastic_ml_index), "Elastic index to add the ML provenance state data to.")
        ("elastic_batch",
         po::value<int>(&elastic_batch_size)->default_value(elastic_batch_size),
         "Elastic batch size in bytes for bulk requests")
        ("ewait_time",
         po::value<int>(&elastic_issue_time)->default_value(elastic_issue_time),
         "time to wait in miliseconds before issuing a bulk request to Elasticsearch if the batch size wasn't reached")
        ("lru_cap", po::value<int>(&lru_cap)->default_value(lru_cap), "LRU Cache max capacity")
        ("prov_file_lru_cap", po::value<int>(&prov_file_lru_cap)->default_value(prov_file_lru_cap), "Prov File LRU Cache max capacity")
        ("prov_core_lru_cap", po::value<int>(&prov_core_lru_cap)->default_value(prov_core_lru_cap), "Prov Core LRU Cache max capacity")
        ("recovery", po::value<bool>(&recovery)->default_value(recovery),
         "enable or disable startup recovery")
        ("stats", po::value<bool>(&stats)->default_value(stats),
         "enable or disable the metrics server")
        ("metricsServer", po::value<std::string>(&metricsServer)->default_value
            (metricsServer),"binding ip and port for the metrics server")
        ("barrier", po::value<int>()->default_value(barrier),
         "Table tailer barrier type. EPOCH=0, GCI=1")
        ("reindex", po::value<bool>(&reindex)->default_value(reindex),
         "initialize an empty index with all metadata")
        ("reindex_of", po::value<std::string>(&reindex_of)->default_value(reindex_of),
         "define index to initialize [project, featurestore]")
        ("ssl_enabled", po::value<bool>(&sslEnabled)->default_value(sslEnabled),
         "ssl enabled or disabled for elastic communication")
        ("ca_path", po::value<std::string>(&caPath)->default_value(caPath),
        "path to the ca cert used to verify the elastic server")
        ("username", po::value<std::string>(&username)->default_value(username),
         "username to access the elastic server")
        ("password", po::value<std::string>(&password)->default_value(password),
         "password to access the elastic server")
        ("log_level", po::value<int>()->default_value(log_level),
         "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
        ("log_rotation_size", po::value<int>(&log_rotation_size)->default_value(log_rotation_size),
         "log rotation size in bytes.")
        ("log_dir", po::value<std::string>(&log_dir)->default_value(log_dir),
         "log directory.")
        ("log_max_files", po::value<int>(&log_max_files)->default_value(log_max_files),
         "max number of log files to keep.");


    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, generalOptions), vm);
    po::notify(vm);

    if (vm.count("help")) {
      std::cout << generalOptions << std::endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("desc")) {
      std::cout << fileOptions << std::endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("version")) {
      std::cout << "ePipe " << EPIPE_VERSION_MAJOR << "." << EPIPE_VERSION_MINOR
           << "." << EPIPE_VERSION_BUILD << std::endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("config")) {
      std::ifstream ifs(vm["config"].as<std::string>().c_str());
      if (ifs)
        po::store(po::parse_config_file(ifs, fileOptions), vm);
    }
    po::notify(vm);


    if (vm.count("fs_mutations_tu")) {
      mutations_tu.update(vm["fs_mutations_tu"].as<std::vector<int> >());
    }

    if (vm.count("schamebased_tu")) {
      schamebased_tu.update(vm["schamebased_tu"].as<std::vector<int> >());
    }

    if (vm.count("provenance_tu")) {
      provenance_tu.update(vm["provenance_tu"].as<std::vector<int> >());
    }

    if (vm.count("barrier")) {
      barrier = static_cast<Barrier> (vm["barrier"].as<int>());
    }

    if (vm.count("log_level")) {
      log_level = static_cast<LogSeverityLevel> (vm["log_level"].as<int>());
    }

    std::string log_prefix = reindex ? "epipe_reindex" : "epipe"; 
    Logger::initLogging(log_prefix,log_dir, log_rotation_size, log_max_files, log_level);

    if (connection_string.empty() || database_name.empty() ||
        meta_database_name.empty()) {
      LOG_ERROR(
          "you should provide at least connection, database, meta_database");
      return EXIT_FAILURE;
    }

    HttpClientConfig config = {elastic_addr, sslEnabled, caPath, username,
                               password};
    if (reindex) {
      if(reindex_of == "project") {
        LOG_INFO("Create Elasticsearch index at " << elastic_index);
        Reindexer *reindexer = new Reindexer(connection_string.c_str(),
                                             database_name.c_str(),
                                             meta_database_name.c_str(),
                                             hive_meta_database_name.c_str(),
                                             config, elastic_index, elastic_batch_size,
                                             elastic_issue_time, lru_cap);
        reindexer->run();
      } else if(reindex_of == "featurestore") {
        LOG_INFO("Create Elasticsearch index at " << elastic_featurestore_index);
        FeaturestoreReindexer *reindexer = new FeaturestoreReindexer(connection_string.c_str(),
                database_name.c_str(), meta_database_name.c_str(), hive_meta_database_name.c_str(), config,
                elastic_featurestore_index, elastic_batch_size, elastic_issue_time, lru_cap);
        reindexer->run();
      } else if(reindex_of == "all") {
        LOG_INFO("Create Elasticsearch index at " << elastic_index);
        Reindexer *projectReindexer = new Reindexer(connection_string.c_str(),
                database_name.c_str(), meta_database_name.c_str(), hive_meta_database_name.c_str(),
                config, elastic_index, elastic_batch_size, elastic_issue_time, lru_cap);
        projectReindexer->run();
        LOG_INFO("Create Elasticsearch index at " << elastic_featurestore_index);
        FeaturestoreReindexer *featurestoreReindexer = new FeaturestoreReindexer(connection_string.c_str(),
                database_name.c_str(), meta_database_name.c_str(), hive_meta_database_name.c_str(), config,
                elastic_featurestore_index, elastic_batch_size, elastic_issue_time, lru_cap);
        featurestoreReindexer->run();
      }
    } else {
      Notifier *notifer = new Notifier(connection_string.c_str(),
                                       database_name.c_str(),
                                       meta_database_name.c_str(),
                                       hive_meta_database_name.c_str(),
                                       mutations_tu, schamebased_tu,
                                       provenance_tu,
                                       poll_maxTimeToWait, config,
                                       hopsworks, elastic_index, elastic_featurestore_index,
                                       elastic_app_provenance_index, elastic_ml_index,
                                       elastic_batch_size, elastic_issue_time,
                                       lru_cap, prov_file_lru_cap, prov_core_lru_cap,
                                       recovery, stats, barrier,
                                       hiveCleaner, metricsServer);
      notifer->start();
    }
    return EXIT_SUCCESS;
  }catch(const po::error &ex){
    LOG_ERROR(ex.what());
  }
}
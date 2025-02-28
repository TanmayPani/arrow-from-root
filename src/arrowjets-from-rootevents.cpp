#include "fastjet/ClusterSequence.hh"

#include <TDatabasePDG.h>

#include "arrow-serializer/writer.hh"
#include "arrowjets-from-rootevents.hh"
#include "genEvent.hh"
#include <filesystem>
arrow::Status
arrowjets_from_rootevents(ROOT::RDF::RNode inputRNode,
                          const std::string &outFilePath = "jets.arrow",
                          unsigned int batchSize = 10) {
  fastjet::JetDefinition jetDef(fastjet::antikt_algorithm, 0.4,
                                fastjet::BIpt2_scheme);
  fastjet::Selector constituentSelector = fastjet::SelectorAbsEtaMax(1.);
  fastjet::Selector jetSelector =
      fastjet::SelectorAbsEtaMax(0.6) && fastjet::SelectorPtMin(10);

  unsigned long nEvents = 0;
  unsigned long nJets = 0;
  unsigned int nBatches = 0;

  arrow::MemoryPool *pool = arrow::default_memory_pool();

  DataSerializer writer(outFilePath, batchSize, pool);

  auto _fill = [&](genEvent &event) {
    nEvents++;
    // cout<<"Event number: "<<iEvent<<endl;
    double _weight = event.weight;
    unsigned int _index = 0;
    std::vector<fastjet::PseudoJet> _constituents = {};
    for (auto &_particle : event.particles) {
      _index++;
      _constituents.emplace_back(_particle.px(), _particle.py(), _particle.pz(),
                                 _particle.e());
      _constituents.back().set_user_index(_index - 1);
      // cout<<"______addded particle: "<<_index<<" pt: "<<particle.pt()<<" eta:
      // "<<particle.eta()<<" phi: "<<particle.phi()<<endl;
    }

    fastjet::ClusterSequence _clusterSeq(constituentSelector(_constituents),
                                         jetDef);
    std::vector<fastjet::PseudoJet> jets =
        fastjet::sorted_by_pt(jetSelector(_clusterSeq.inclusive_jets(1.0)));

    if (jets.empty())
      return;

    for (auto &jet : jets) {
      unsigned int _nCharged = 0, _nNeutral = 0;
      if (jet.constituents().size() < 2)
        continue;
      double _pt = jet.pt();
      double _eta = jet.eta();
      double _phi = jet.phi();
      double _e = jet.e();
      // std::cout<<"Event: "<<nEvents<<" Jet: "<<nJets<<" pt: "<<jet.pt()<<"
      // eta: "<<jet.eta()<<" phi: "<<jet.phi()<<" e: "<<jet.e()<<std::endl;
      std::vector<double> _con_pt, _con_eta, _con_phi, _con_e;
      std::vector<int> _con_pid, _con_charge;
      double _nef = 0;
      auto _jetConstituents = fastjet::sorted_by_pt(jet.constituents());
      for (auto &_constituent : _jetConstituents) {
        int _user_index = _constituent.user_index();
        int _pdgCode = event.particle_PDGIds[_user_index];
        TParticlePDG *_particlePDGInfo =
            TDatabasePDG::Instance()->GetParticle(_pdgCode);
        short _charge = (_particlePDGInfo->Charge()) / 3;
        if (_charge != 0) {
          _nCharged++;
        } else {
          _nNeutral++;
          _nef += _constituent.e();
        }

        _con_pid.push_back(_pdgCode);
        _con_charge.push_back(_charge);

        _con_pt.push_back(_constituent.pt());
        _con_eta.push_back(_constituent.eta());
        _con_phi.push_back(_constituent.phi());
        _con_e.push_back(_constituent.e());
      }

      if (_nCharged < 2)
        continue;

      nJets++;

      _nef = _nef / _e;

      writer["wt"]->Append(_weight);
      writer["pt"]->Append(_pt);
      writer["eta"]->Append(_eta);
      writer["phi"]->Append(_phi);
      writer["e"]->Append(_e);
      writer["nef"]->Append(_nef);
      writer["ncon_charged"]->Append(_nCharged);
      writer["ncon_neutral"]->Append(_nNeutral);
      writer["con_pt"]->Append(_con_pt);
      writer["con_eta"]->Append(_con_eta);
      writer["con_phi"]->Append(_con_phi);
      writer["con_e"]->Append(_con_e);
      writer["con_pid"]->Append(_con_pid);
      writer["con_charge"]->Append(_con_charge);
    }
  };

  inputRNode.Foreach(_fill, {"event"});

  return arrow::Status::OK();
}

arrow::Status arrowjets_from_rootevents(
    const std::string &inTreeName, const std::vector<std::string> &inFilePaths,
    const std::string &outFilePath, unsigned int batchSize) {

  ROOT::RDataFrame inTree(inTreeName, inFilePaths, {"event"});

  return arrowjets_from_rootevents(inTree, outFilePath, batchSize);
}

arrow::Status arrowjets_from_rootevents(const std::string &inTreeName,
                                        const std::string &inFilePathGlob,
                                        const std::string &outputDir,
                                        unsigned int batchSize) {
  if (!std::filesystem::exists(outputDir)) {
    std::filesystem::create_directory(outputDir);
  }

  std::cout << "Events will be read from: " << inFilePathGlob << std::endl;

  std::string inFileName =
      inFilePathGlob.substr(inFilePathGlob.find_last_of("/\\") + 1);
  std::string inFileDir =
      inFilePathGlob.substr(0, inFilePathGlob.find_last_of("/\\"));
  std::string inFileNameBase = "";
  if (inFileName.find_last_of("*") != std::string::npos) {
    inFileNameBase = inFileName.erase(inFileName.find_last_of("*"));
  } else {
    inFileNameBase = inFileName.erase(inFileName.find_last_of("."));
  }

  std::vector<std::string> inFilePaths;

  for (auto &file : std::filesystem::directory_iterator(inFileDir)) {
    std::string _filePath = file.path().string();
    if (_filePath.find(inFileNameBase) != std::string::npos) {
      std::cout << "Found file: " << file.path().string() << std::endl;
      inFilePaths.push_back(file.path().string());
    }
  }

  std::string outFileName = "jetTable-" + inFileNameBase + ".arrow";
  std::string outFilePath = outputDir + "/" + outFileName;
  std::cout << "Jets will be saved to: " << outFilePath << std::endl;

  return arrowjets_from_rootevents(inTreeName, inFilePaths, outFilePath,
                                   batchSize);
}

#include "arrowjets-from-rootevents.hh"

int main(int argc, char* argv[]) {
    std::string inFilePath = (argc > 1) ? argv[1] : "/mnt/d/JEWEL/pp200GeV_dijet.root";
    std::string treeName = (argc > 2) ? argv[2] : "hepmc2Tree";
    std::string outputDir = (argc > 3) ? argv[3] : "/home/tanmaypani/workspace/macros/output";
    unsigned int batchSize = (argc > 4) ? std::stoi(argv[4]) : 100000;

    auto status = arrowjets_from_rootevents(treeName, inFilePath, outputDir, batchSize);
    if (!status.ok()) {
        std::cerr << "Error occurred : " << status.message() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout<<"Done!"<<std::endl;
    return EXIT_SUCCESS;
}

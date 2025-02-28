#ifndef genEvent_hh
#define genEvent_hh

#include "Math/Vector3D.h"
#include "Math/Vector4D.h"
#include <vector>

class genEvent{
public:
    genEvent(){}
    virtual ~genEvent(){}

    void reset(){
        index = 0;
        weight = 1.0;
        nParticles = 0;
        particles.clear();
        particle_PDGIds.clear();
    }
    
    unsigned int index = 0;
    double weight = 1.0;
    unsigned int nParticles = 0;
    std::vector<ROOT::Math::PxPyPzEVector> particles = {};
    std::vector<int> particle_PDGIds = {};
};


#endif
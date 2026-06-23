from dataclasses import dataclass


@dataclass(frozen=True)
class Run_context:
    slot : str
    submission : str
    dataprovider : str
    root : str
    runid : str
    

    def get_slot(self):
        return self.slot
    
    def get_submission(self):
        return self.submission
    
    def get_root(self):
        return self.root
    
    def get_dateprovider(self):
        return self.dataprovider
    
    
    




    
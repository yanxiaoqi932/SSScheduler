from typing import List
import random
from time import perf_counter
import string
from math import exp, floor
import pickle

class ReqStructure():
    def __init__(self) -> None:
        self.RequestID: int
        self.RequestType: string
        self.SLA: int
        self.Driver: List[int]
        self.RequestSize: int
        self.LogicalClock: int
        # parameters define by ourselves
        self.now_sla: int
        self.type: string
        self.score: float
        self.selected_driver: int

class OperReq():
    """
    a structure which is convenient for operation.
    """
    def __init__(self) -> None:
        self.Driver: List[int]
        self.RequestSize: int
        self.score: float
        self.id: int
        self.selected_driver: int

class alns():
    def __init__(self, ini_requests:List[ReqStructure], remain_cap:List[int], 
    ini_score:float, max_iteraion:int, max_runtime:float, remain_time:float, real_cap:List[int], 
    start_temp:float, end_temp:float, temp_step:float, temp_s1:float) -> None:
        """
        ini_requests: requests dealed with wfac-bin-packing
        remain_cap: driver remain capacity after wfac-bin-packing
        ini_score: sum of score about wfac-bin-packing
        real_cap: real max capacity of a driver
        """
        self.num_oper = 2
        self.num_driver = len(remain_cap)
        self.real_cap = pickle.loads(pickle.dumps((real_cap)))
        self.max_iteration = max_iteraion
        self.max_runtime = min(max_runtime, remain_time)
        self.oper_weight: List[int] = [1] * self.num_oper
        self.oper_prob: List[float] = [1 / self.num_oper] * self.num_oper
        self.iteraion = 0
        self.runtime = 0

        self.start_temp = start_temp
        self.end_temp = end_temp
        self.temp = pickle.loads(pickle.dumps((self.start_temp)))
        self.step = temp_step
        self.s1 = temp_s1    # ajust to bigger, acception prob will be smaller

        self.best_score = pickle.loads(pickle.dumps((ini_score)))    # score of best solution
        self.score = pickle.loads(pickle.dumps((ini_score)))         # score of previous solution
        self.best_reqs = pickle.loads(pickle.dumps((ini_requests)))
        self.requests = pickle.loads(pickle.dumps((ini_requests)))    # reqs of previous solution, can't change its sequence, only change its selected_driver
        self.best_remain_cap = pickle.loads(pickle.dumps((remain_cap)))
        self.remain_cap = pickle.loads(pickle.dumps((remain_cap)))

        self.up_reqs: List[List[OperReq]] = []   # reqs on each driver, only use it in operations
        self.down_reqs: List[OperReq] = [] # reqs waiting, only use it in operations, sort from big to small according to their score
        self.ini_on_down_req()
    
    def iteration_alns(self):
        start_time = perf_counter()
        while not self.is_stop():
            oper_id = self.choose_prob(self.oper_prob) 
            cand_up_reqs, cand_down_reqs, cand_remain_cap, cand_score = self.operaion(oper_id=oper_id)
            if self.is_accepted(score=cand_score):
                # update parameters
                self.up_reqs = cand_up_reqs
                self.down_reqs = cand_down_reqs
                self.remain_cap = cand_remain_cap
                self.score = cand_score
                self.OperReq2ReqStrut(cand_up_reqs=cand_up_reqs, cand_down_reqs=cand_down_reqs) # update self.requests
                self.update_weight_prob(oper_id=oper_id, step=1)
            if cand_score > self.best_score:
                self.best_reqs = pickle.loads(pickle.dumps((self.requests)))
                self.best_score = pickle.loads(pickle.dumps((self.score)))
                self.best_remain_cap = pickle.loads(pickle.dumps((self.remain_cap)))
                self.update_weight_prob(oper_id=oper_id, step=5)

            self.iteraion += 1
            self.runtime = perf_counter() - start_time
            self.temp = max(self.end_temp, self.temp * self.step)
        return self.best_reqs, self.best_score, self.best_remain_cap
            
    
    def update_weight_prob(self, oper_id:int, step:int):
        self.oper_weight[oper_id] += step
        s = sum(self.oper_weight)
        self.oper_prob = [float(w / s) for w in self.oper_weight]

    def choose_prob(self, prob:List[float]) -> int:
        """
        choose a element according to probability list.

        return: element index
        """
        p = random.random()
        s = 0
        for i in range(len(prob)):
            s += prob[i]
            if s >= p:
                return i
        return 0

    def ini_on_down_req(self):
        for _ in range(len(self.remain_cap)):
            self.up_reqs.append([])
        for i in range(len(self.requests)):
            r = self.requests[i]
            oper_req = OperReq()
            oper_req.id = i
            oper_req.Driver = r.Driver
            oper_req.RequestSize = r.RequestSize
            oper_req.score = r.score
            oper_req.selected_driver = r.selected_driver
            if r.selected_driver != -1:
                self.up_reqs[r.selected_driver].append(oper_req)
            else:
                self.down_reqs.append(oper_req)

    def OperReq2ReqStrut(self, cand_up_reqs:List[List[OperReq]], cand_down_reqs:List[OperReq]):
        """
        change the self.requests' selected_driver  through cand_up_reqs and cand_down_reqs
        """
        for driver_reqs in cand_up_reqs:
            for r in driver_reqs:
                self.requests[r.id].selected_driver = r.selected_driver
        for r in cand_down_reqs:
            self.requests[r.id].selected_driver = r.selected_driver

    def operaion(self, oper_id: int):
        """
        excuting opraion through the oper_id.
        """
        if oper_id == 0:
            return self.operation_0()
        elif oper_id == 1:
            return self.operation_1()
        else:
            raise IndexError("Error: operaion id is not exist")

    def is_accepted(self, score:float) -> bool:
        """
        judging whether the candidate solution is need to be accepted,
        we use hill climbing in there.

        return: true means the solution is accepted.
        """
        if score >= self.score:
            return True
        else:
            d_score = score - self.score
            p = exp((d_score / self.temp) * self.s1)
            return random.random() <= p 

    def is_stop(self):
        """
        judging whether the iteraion solution is need to be stopped.
        """
        if self.max_runtime > self.runtime:
            return False
        else:
            return True

    
    def prob_ger_req(self, req_id_list:List[int], driver_reqs:List[OperReq]) -> int:
        """
        compute each req's probability be chosen in the list, according to their RequestSize, 
        and then get a req through the probability list.

        return: req_id in reqs list
        """
        try:
            sum_size = sum([driver_reqs[i].RequestSize for i in req_id_list])
            prob = [float(driver_reqs[i].RequestSize / sum_size) for i in req_id_list]
            return req_id_list[self.choose_prob(prob=prob)]
        except:
            return req_id_list[0]

    def move_to_other(self, req:OperReq, driver_id:int) -> bool:
        """
        judge if a req's size is bigger than other driver's max_capacity except current driver, 
        if it is, it can't move to other driver.

        return: true means it can move 
        """
        for d in req.Driver:
            if d != driver_id and self.real_cap[d] > req.RequestSize:
                return True
        return False

    def up_add_req(self, driver_cand_up_reqs:List[OperReq], req:OperReq, driver_id:int, driver_cap:int, score:float):
        """
        add new req to a driver's up_reqs list, and update parameters

        return: driver_cand_up_reqs: List[OperReq], driver_cap: int, score: float
        """
        req.selected_driver = driver_id
        driver_cand_up_reqs.append(req)
        driver_cap -= req.RequestSize
        score += req.score
        # if driver_cap < 0:
        #     raise AttributeError(f'up_add_req: {driver_cap}')
        return driver_cand_up_reqs, driver_cap, score

    def up_remove_req(self, driver_cand_up_reqs:List[OperReq], req:OperReq, driver_cap:int, score:float):
        """
        remove a req from a driver's up_reqs list, and update parameters

        return: driver_cand_up_reqs: List[OperReq], driver_cap: int, score: float
        """
        driver_cand_up_reqs.remove(req)
        driver_cap += req.RequestSize
        score -= req.score
        # if driver_cap < 0:
        #     raise AttributeError(f'up_remove_req: {driver_cap}')
        return driver_cand_up_reqs, driver_cap, score

    def down_add_req(self, cand_down_reqs:List[OperReq], req:OperReq):
        """
        add a new req to cand_down_reqs

        return: cand_down_reqs:List[OperReq] 
        """
        s = []
        for i in range(len(cand_down_reqs)):
            if req.score > cand_down_reqs[i].score:
                if i == 0:
                    req.selected_driver = -1; s.append(req); cand_down_reqs = s + cand_down_reqs; break
                else:
                    req.selected_driver = -1; s = cand_down_reqs[:i]; s.append(req); cand_down_reqs = s + cand_down_reqs[i:]; break
        if req.selected_driver != -1:
            req.selected_driver = -1; cand_down_reqs.append(req)
        return cand_down_reqs

    def down_remove_req(self, cand_down_reqs:List[OperReq], driver_cand_up_reqs:List[OperReq], driver_id:int, driver_cap:int, cand_score:int):
        """
        move some reqs of waiting list to a driver's up_reqs lists

        return: cand_down_reqs: List[OperReq], driver_cand_up_reqs: List[OperReq], driver_cap: int, cand_score: int
        """
        for i in range(len(cand_down_reqs)):
            if driver_id in cand_down_reqs[i].Driver and cand_down_reqs[i].RequestSize < driver_cap:
                cand_down_reqs[i].selected_driver = driver_id
                driver_cand_up_reqs.append(cand_down_reqs[i])
                driver_cap -= cand_down_reqs[i].RequestSize
                cand_score += cand_down_reqs[i].score
        # delete reqs move to drivers
        for i in range(len(cand_down_reqs)-1, -1, -1):
            if cand_down_reqs[i].selected_driver != -1:
                cand_down_reqs.pop(i)
        # if driver_cap < 0:
        #     raise AttributeError(f'down_remove_req: {driver_cap}')
        return cand_down_reqs, driver_cand_up_reqs, driver_cap, cand_score

    def operation_0(self):
        """
        BPR(Big Process Remove) operation:
        for one driver, put a big size req down, try to put waiting reqs to this driver

        return: candidate driver_reqs: List[List[req]], candidate down_reqs: List[req], 
                candidate driver_remain_cap: List[int], candidate score: float
        """
        cand_up_reqs = pickle.loads(pickle.dumps((self.up_reqs)))
        cand_down_reqs = pickle.loads(pickle.dumps((self.down_reqs)))
        cand_remain_cap = pickle.loads(pickle.dumps((self.remain_cap)))
        cand_score = pickle.loads(pickle.dumps((self.score)))

        driver_id = random.randint(0, self.num_driver-1)

        if len(cand_up_reqs[driver_id]) > 1:
            num_deal_req = max(1, random.choice([1, floor(0.25 * len(cand_up_reqs[driver_id])), \
                floor(0.5 * len(cand_up_reqs[driver_id]))]))
        elif len(cand_up_reqs[driver_id]) == 1:
            num_deal_req = 1
        else:
            return [], [], [], -100 # if the driver has not any reqs match condition. return immediately
        
        req_id_list: List[int] = list(range(len(cand_up_reqs[driver_id])))
        req_list: List[OperReq] = []
        for _ in range(num_deal_req):
            req_id = self.prob_ger_req(req_id_list, cand_up_reqs[driver_id])
            req = cand_up_reqs[driver_id][req_id]
            req_list.append(req)
            req_id_list.remove(req_id)
        
        for req in req_list:
            cand_up_reqs[driver_id], cand_remain_cap[driver_id], cand_score = \
                self.up_remove_req(cand_up_reqs[driver_id], req, cand_remain_cap[driver_id], cand_score)
        # down_reqs move req to driver
        cand_down_reqs, cand_up_reqs[driver_id], cand_remain_cap[driver_id], cand_score = \
            self.down_remove_req(cand_down_reqs, cand_up_reqs[driver_id], driver_id, cand_remain_cap[driver_id], cand_score)
        # insert req into down_reqs in order
        for req in req_list:
            cand_down_reqs = self.down_add_req(cand_down_reqs, req)

        for c in cand_remain_cap:
            if c < 0:
                raise AttributeError(f'oper_0: {cand_remain_cap}')

        return cand_up_reqs, cand_down_reqs, cand_remain_cap, cand_score
        

    def operation_1(self) -> List[ReqStructure]:
        """
        shift operation:
        for one driver, put a big size req move to another driver, 
        if the target driver capacity is over, try to move some reqs to other drivers, or put into waiting reqs list,
        after that, try to put waiting reqs to two drivers.

        return: candidate driver_reqs: List[List[req]], candidate down_reqs: List[req], 
                candidate driver_remain_cap: List[int], candidate score: float
        """
        cand_up_reqs = pickle.loads(pickle.dumps((self.up_reqs)))
        cand_down_reqs = pickle.loads(pickle.dumps((self.down_reqs)))
        cand_remain_cap = pickle.loads(pickle.dumps((self.remain_cap)))
        cand_score = pickle.loads(pickle.dumps((self.score)))

        driver_id = random.randint(0, self.num_driver-1)
        # find a req in driver's reqs according to its size, and remove it
        max_req_list: List[OperReq] = []
        for i in range(len(cand_up_reqs[driver_id])):
            r = cand_up_reqs[driver_id][i]
            if len(r.Driver) > 1 and self.move_to_other(r, driver_id):
                max_req_list.append(r)
        if max_req_list == []:  # if the driver has not any reqs match condition. return immediately
            return [], [], [], -100

        if len(max_req_list) > 1:
            num_deal_req = max(1, random.choice([1, floor(0.25 * len(max_req_list)), floor(0.5 * len(max_req_list))]))
        else:
            num_deal_req = 1
        
        req_id_list: List[int] = list(range(len(max_req_list)))
        req_list: List[OperReq] = []
        for _ in range(num_deal_req):
            req_id = self.prob_ger_req(req_id_list, max_req_list)
            req = max_req_list[req_id]
            req_list.append(req)
            req_id_list.remove(req_id)

        removed_r: List[OperReq] = [] # collet reqs be removed by to_driver_id
        # move chosen reqs from driver_id to to_driver_ids 
        for max_req in req_list:
            # driver id remove max_req
            cand_up_reqs[driver_id], cand_remain_cap[driver_id], cand_score = \
                self.up_remove_req(cand_up_reqs[driver_id], max_req, cand_remain_cap[driver_id], cand_score)

            # find the driver max_req moved to
            max_req_driverlist = pickle.loads(pickle.dumps((max_req.Driver)))
            max_req_driverlist.remove(driver_id)
            if len(max_req_driverlist) > 1:
                for i in range(len(max_req_driverlist)-1, -1, -1):
                    d = max_req_driverlist[i]
                    if self.real_cap[d] < max_req.RequestSize:
                        max_req_driverlist.pop(i)
                to_driver_id = random.choice(max_req_driverlist)
            else:
                to_driver_id = max_req_driverlist[0]
            if self.real_cap[to_driver_id] < max_req.RequestSize:
                raise AttributeError('self.real_cap[to_driver_id] < max_req.RequestSize')
            
            cand_up_reqs[to_driver_id], cand_remain_cap[to_driver_id], cand_score = \
                self.up_add_req(cand_up_reqs[to_driver_id], max_req, to_driver_id, cand_remain_cap[to_driver_id], cand_score)
            
            # if to_driver_id's capacity is too less, try to move some reqs to other drivers
            if cand_remain_cap[to_driver_id] < 0:
                for i in range(len(cand_up_reqs[to_driver_id])-1, -1, -1):
                    if cand_remain_cap[to_driver_id] >= 0:
                        break

                    r = cand_up_reqs[to_driver_id][i]
                    if r.id == max_req.id:
                        continue

                    if len(r.Driver) > 1:
                        max_cap = -100
                        max_d = -1
                        for d in r.Driver:
                            if cand_remain_cap[d] > r.RequestSize and d != to_driver_id and cand_remain_cap[d] > max_cap:
                                max_cap = cand_remain_cap[d]
                                max_d = d
                        if max_d != -1: 
                            # move the request r from to_driver_id to max_d
                            cand_up_reqs[to_driver_id], cand_remain_cap[to_driver_id], cand_score = \
                                self.up_remove_req(cand_up_reqs[to_driver_id], r, cand_remain_cap[to_driver_id], cand_score)
                            cand_up_reqs[max_d], cand_remain_cap[max_d], cand_score = \
                                self.up_add_req(cand_up_reqs[max_d], r, max_d, cand_remain_cap[max_d], cand_score)
            # if to_driver_id's capacity is still too less, remove some reqs to the down list
            if cand_remain_cap[to_driver_id] < 0:
                for i in range(len(cand_up_reqs[to_driver_id])-1, -1, -1):
                    r = cand_up_reqs[to_driver_id][i]
                    if cand_remain_cap[to_driver_id] >= 0: break
                    if r.id == max_req.id: continue
                    cand_up_reqs[to_driver_id], cand_remain_cap[to_driver_id], cand_score = \
                        self.up_remove_req(cand_up_reqs[to_driver_id], r, cand_remain_cap[to_driver_id], cand_score)
                    removed_r.append(r)
            if cand_remain_cap[to_driver_id] < 0:
                raise AttributeError(f'{cand_remain_cap}, num_req:{len(cand_up_reqs[to_driver_id])}')

        # move reqs from down to up
        for i in range(len(cand_down_reqs)):
            max_cap = -100
            max_d = -1
            for d in [driver_id, to_driver_id]:
                if d in cand_down_reqs[i].Driver and cand_remain_cap[d] > cand_down_reqs[i].RequestSize and \
                    cand_remain_cap[d] > max_cap:
                    max_cap = cand_remain_cap[d]
                    max_d = d
            if max_d != -1:
                cand_down_reqs[i].selected_driver = max_d
                cand_up_reqs[max_d], cand_remain_cap[max_d], cand_score = \
                    self.up_add_req(cand_up_reqs[max_d], cand_down_reqs[i], max_d, cand_remain_cap[max_d], cand_score)
        for i in range(len(cand_down_reqs)-1, -1, -1):
            if cand_down_reqs[i].selected_driver != -1:
                cand_down_reqs.pop(i)
        
        # reqs be remove by to_driver_id, insert into down reqs
        for r in removed_r:
                cand_down_reqs = self.down_add_req(cand_down_reqs, r)
        
        for c in cand_remain_cap:
            if c < 0:
                raise AttributeError(f'oper_2: {cand_remain_cap}')

        return cand_up_reqs, cand_down_reqs, cand_remain_cap, cand_score




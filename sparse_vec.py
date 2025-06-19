import requests, time


def call_sparse_vec_api(self, batch_text: list[str]):
    data = {
        "inputs": batch_text,
    }
    for _ in range(3):
        try:
            resp = requests.post("http://10.12.160.250:9200/embed_sparse", json=data, timeout=120)
            if resp.status_code == 200:
                res_li = resp.json()
                assert len(batch_text) == len(res_li)
                return res_li
            else:
                continue
        except Exception as e:
            print("exception:", e)
            time.sleep(0.2)
            continue
    return None


if __name__ == "__main__":
    ret = call_sparse_vec_api(["This is a test document.", "Another test document for sparse vector enrichment."])
    print(ret)
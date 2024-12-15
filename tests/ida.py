# from pyeclib.ec_iface import ECDriver
import os
import unittest
from mictlanxrouter.helpers.utils import Utils

class PreprocessingTest(unittest.TestCase):
    @unittest.skip("")
    def test_disperse_file(self):
        result = Utils.disperse_file(
            data= b"HOLA",
            base_id="mydata",
            ec_type="isa_l_rs_vand",
            n=10,
            k=2,
            output_dir="/sink/fragments"
        )
        return self.assertTrue(result.base_id=="mydata")
    # @unittest.skip("")
    def test_reconstruct_file(self):
        fragments = []
        for i in [0, 6, 9]:  # Using fragment indices 0, 2, and 4
            fragments.append(f'/sink/fragments/mydata_{i}.part')

        result = Utils.reconstruct_file(
            output_file="/sink/x",
            ec_type="isa_l_rs_vand",
            fragments_path=fragments,
            k=2,
            m=8
        )
        print("RESULT", result)
    def test_preprocessing_file(self):
        pass

if __name__ =="__main__":
    unittest.main()
# def disperse_file(file_path, n, k, output_dir, ec_type:str = "jerasure_rs_vand"):
#     """Disperse a file into n fragments, with k required to reconstruct."""
    
#     # Create an erasure coding driver for Reed-Solomon (RS)
#     m = n-k
#     ec_driver = ECDriver(k=k, m=m, ec_type= ec_type)  # k = data parts, m = parity parts
    
#     # Read the file data
#     with open(file_path, 'rb') as f:
#         file_data = f.read()
    
#     # Encode the file data into n fragments
#     encoded_fragments = ec_driver.encode(file_data)

#     # Save the fragments to files
#     os.makedirs(output_dir, exist_ok=True)
#     for i, fragment in enumerate(encoded_fragments):
#         fragment_path = os.path.join(output_dir, f"fragment_{i}.part")
#         with open(fragment_path, 'wb') as f:
#             f.write(fragment)

#     return len(encoded_fragments), len(encoded_fragments[0])
# def reconstruct_file(fragments, output_file, k,m:int, ec_type:str = "jerasure_rs_vand"):
#     """Reconstruct a file from k fragments."""
    
#     # Create an erasure coding driver for Reed-Solomon (RS)
#     ec_driver = ECDriver(k=k, m=m, ec_type=ec_type)
    
#     # Reconstruct the original file from the fragments
#     reconstructed_data = ec_driver.decode(fragments)
    
#     # Save the reconstructed file
#     with open(output_file, 'wb') as f:
#         f.write(reconstructed_data)

#     print(f"File reconstructed successfully as {output_file}")
# import os

# if __name__ == "__main__":
#     original_file = '/sink/mictlanx-sync/jcastillo/CIAT_ABE___Cluster_Computing.pdf'
#     output_dir = '/sink/test/'

#     # Parameters for dispersal
#     n = 5  # Total number of fragments
#     k = 3  # Minimum number of fragments needed to reconstruct

#     # Disperse the file into n fragments, with k needed to reconstruct
#     # total_fragments, fragment_size = disperse_file(original_file, n, k, output_dir,ec_type="isa_l_rs_vand")
#     # print(f"File dispersed into {total_fragments} fragments.")

#     # Simulate using only k fragments for reconstruction
#     fragments_to_use = []
#     for i in [0, 2, 1]:  # Using fragment indices 0, 2, and 4
#         with open(f'/sink/test/fragment_{i}.part', 'rb') as f:
#             fragments_to_use.append(f.read())

#     # Reconstruct the file using only k fragments
#     reconstructed_file = '/sink/reconstructed_file.pdf'
#     reconstruct_file(fragments_to_use, reconstructed_file, k,n-k, ec_type="isa_l_rs_vand")


import json,time,unittest
from unittest.mock import patch
from supercomputer.namespace_recovery_transport import NativeTransport,CAPSULE,BOOT

class NativeTransportTests(unittest.TestCase):
 def test_scripts_compile(self):
  compile(CAPSULE,'capsule','exec');compile(BOOT,'boot','exec')
 def test_aliases_never_reach_provider_or_guest(self):
  with patch('supercomputer.namespace_recovery_transport.boat.bj') as info:
   for vm in ('current','self','friendly-name','host.example','bx_id/../../other'):
    with self.assertRaises(ValueError):NativeTransport()._exec(vm,'pass',{},time.monotonic()+20)
   info.assert_not_called()
 def test_wrong_provider_id_or_foreign_access_refuses_guest_read(self):
  for state in ({'id':'bx_other','access':'owner'},{'id':'bx_target','access':'viewer'}):
   with patch('supercomputer.namespace_recovery_transport.boat.bj',return_value={'sandbox':state}),patch('supercomputer.namespace_recovery_transport.boat.boat') as execute:
    with self.assertRaises(ValueError):NativeTransport()._exec('bx_target','pass',{},time.monotonic()+20)
    execute.assert_not_called()
 def test_authenticated_immutable_id_uses_fresh_encoded_request(self):
  with patch('supercomputer.namespace_recovery_transport.boat.bj',return_value={'sandbox':{'id':'bx_target','access':'owner'}}),patch('supercomputer.namespace_recovery_transport.boat.boat',return_value=json.dumps({'nonce':'fresh'})) as execute:
   self.assertEqual(NativeTransport()._exec('bx_target','pass',{'nonce':'fresh'},time.monotonic()+20),{'nonce':'fresh'})
   self.assertIn('bx_target',execute.call_args.args)
 def test_stop_never_uses_unfenced_or_delete_fallback(self):
  with patch('supercomputer.namespace_recovery_transport.boat.boat') as execute:
   for fence in ({},{'state':'held','stop_nonce':'n'},{'state':'stopping'}):
    with self.assertRaises(ValueError):NativeTransport().stop_vm('bx_target',fence,preserve_disk=True,deadline=time.monotonic()+20)
   execute.assert_not_called()
if __name__=='__main__':unittest.main()

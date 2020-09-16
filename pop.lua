local safequeue = require 'safequeue'


local info = safequeue.pop();

if (info ~= nil) then
	for k,v in pairs(info) do
		print(k,v)
	end
end

